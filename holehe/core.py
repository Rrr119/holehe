from bs4 import BeautifulSoup
from termcolor import colored
import httpx
import trio

from subprocess import Popen, PIPE
import os
from argparse import ArgumentParser, ArgumentTypeError
import copy
import csv
from datetime import datetime
import time
import importlib
import pkgutil
import hashlib
import re
import sys
import string
import random
import json

from holehe.localuseragent import ua
from holehe.instruments import TrioProgress


try:
    import cookielib
except Exception:
    import http.cookiejar as cookielib


DEBUG        = False
EMAIL_FORMAT = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
DEFAULT_CONCURRENCY = 10
DEFAULT_RATE_LIMIT = 20.0

__version__ = "1.61"


def import_submodules(package, recursive=True):
    """Get all the holehe submodules"""
    if isinstance(package, str):
        package = importlib.import_module(package)
    results = {}
    for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
        full_name = package.__name__ + '.' + name
        results[full_name] = importlib.import_module(full_name)
        if recursive and is_pkg:
            results.update(import_submodules(full_name))
    return results


def get_functions(modules,args=None):
    """Transform the modules objects to functions"""
    websites = []

    for module in modules:
        if len(module.split(".")) > 3 :
            modu = modules[module]
            site = module.split(".")[-1]
            if args is not None and args.nopasswordrecovery==True:
                if  "adobe" not in str(modu.__dict__[site]) and "mail_ru" not in str(modu.__dict__[site]) and "odnoklassniki" not in str(modu.__dict__[site]) and "samsung" not in str(modu.__dict__[site]):
                    websites.append(modu.__dict__[site])
            else:
                websites.append(modu.__dict__[site])
    return websites

def check_update():
    """Check and update holehe if not the last version"""
    check_version = httpx.get("https://pypi.org/pypi/holehe/json")
    if check_version.json()["info"]["version"] != __version__:
        if os.name != 'nt':
            p = Popen(["pip3",
                       "install",
                       "--upgrade",
                       "holehe"],
                      stdout=PIPE,
                      stderr=PIPE)
        else:
            p = Popen(["pip",
                       "install",
                       "--upgrade",
                       "holehe"],
                      stdout=PIPE,
                      stderr=PIPE)
        (output, err) = p.communicate()
        p_status = p.wait()
        print("Holehe has just been updated, you can restart it.")
        exit()

def credit():
    """Print Credit"""
    print('Twitter : @palenath')
    print('Github : https://github.com/megadose/holehe')
    print('For BTC Donations : 1FHDM49QfZX6pJmhjLE5tB2K6CaTLMZpXZ')

def is_email(email: str) -> bool:
    """Check if the input is a valid email address

    Keyword Arguments:
    email       -- String to be tested

    Return Value:
    Boolean     -- True if string is an email, False otherwise
    """

    return bool(re.fullmatch(EMAIL_FORMAT, email))


def deduplicate_emails(emails):
    """Remove duplicate emails while preserving their first spelling and order."""
    seen = set()
    unique = []
    for raw_email in emails:
        email = raw_email.strip()
        key = email.casefold()
        if key not in seen:
            seen.add(key)
            unique.append(email)
    return unique


def read_email_file(path):
    """Read one email address per line, ignoring blank lines and comments."""
    emails = []
    with open(path, "r", encoding="utf-8") as email_file:
        for line in email_file:
            line = line.strip()
            if line and not line.startswith("#"):
                emails.append(line)
    return emails


def collect_emails(args):
    """Collect positional and file-based emails, then deduplicate them."""
    emails = list(args.email or [])
    for path in args.email_file or []:
        emails.extend(read_email_file(path))
    return deduplicate_emails(emails)


def validate_emails(emails):
    """Validate every target before starting any network request."""
    if not emails:
        raise SystemExit(
            "[-] Please enter a target email ! \n"
            "Example : holehe email@example.com"
        )

    invalid = [email for email in emails if not is_email(email)]
    if invalid:
        preview = ", ".join(invalid[:5])
        if len(invalid) > 5:
            preview += ", ..."
        raise SystemExit("[-] Invalid email address(es): " + preview)

    return emails


def _result_score(result):
    if result.get("exists") is True:
        priority = 4
    elif result.get("rateLimit") is True:
        priority = 3
    elif result.get("error") is True:
        priority = 2
    else:
        priority = 1
    populated = sum(
        value is not None for value in result.values()
    )
    return priority, populated


def deduplicate_results(data):
    """Collapse duplicate site records, preferring the most useful result."""
    deduplicated = {}
    for result in data:
        key = (result.get("name"), result.get("domain"))
        current = deduplicated.get(key)
        if current is None or _result_score(result) > _result_score(current):
            deduplicated[key] = result
    return list(deduplicated.values())


def batch_csv_rows(results):
    """Flatten batch results into rows while retaining all module fields."""
    rows = []
    for email, data in results:
        for result in data:
            row = {"email": email}
            row.update(result)
            rows.append(row)
    return rows


def positive_int(value):
    """Argparse type for an integer greater than zero."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise ArgumentTypeError("must be greater than zero")
    return parsed


def non_negative_float(value):
    """Argparse type for a non-negative float."""
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ArgumentTypeError("must be a number") from exc
    if parsed < 0:
        raise ArgumentTypeError("must be zero or greater")
    return parsed


class AsyncRateLimiter:
    """Serialize request starts to a maximum number of requests per second."""

    def __init__(self, rate_limit):
        self.rate_limit = float(rate_limit)
        self._lock = trio.Lock()
        self._next_request_at = None

    async def acquire(self):
        if self.rate_limit <= 0:
            return

        interval = 1.0 / self.rate_limit
        async with self._lock:
            now = trio.current_time()
            if self._next_request_at is None:
                self._next_request_at = now
            delay = self._next_request_at - now
            if delay > 0:
                await trio.sleep(delay)
                now = trio.current_time()
            self._next_request_at = max(now, self._next_request_at) + interval


class RateLimitedAsyncClient(httpx.AsyncClient):
    """An httpx client that applies a global request rate limit."""

    def __init__(self, *args, rate_limit=0, **kwargs):
        self._rate_limiter = AsyncRateLimiter(rate_limit)
        super().__init__(*args, **kwargs)

    async def _send_single_request(self, request):
        await self._rate_limiter.acquire()
        return await super()._send_single_request(request)


def print_result(data,args,email,start_time,websites):
    def print_color(text,color,args):
        if args.nocolor == False:
            return(colored(text,color))
        else:
            return(text)

    description = print_color("[+] Email used","green",args) + "," + print_color(" [-] Email not used", "magenta",args) + "," + print_color(" [x] Rate limit","yellow",args) + "," + print_color(" [!] Error","red",args)
    if args.noclear==False:
        print("\033[H\033[J")
    else:
        print("\n")
    print("*" * (len(email) + 6))
    print("   " + email)
    print("*" * (len(email) + 6))

    for results in data:
        if results["rateLimit"] and args.onlyused == False:
            websiteprint = print_color("[x] " + results["domain"], "yellow",args)
            print(websiteprint)
        elif "error" in results.keys() and results["error"] and args.onlyused == False:
            toprint = ""
            if results["others"] is not None and "Message" in str(results["others"].keys()):
                toprint = " Error message: " + results["others"]["errorMessage"]
            websiteprint = print_color("[!] " + results["domain"] + toprint, "red",args)
            print(websiteprint) 
        elif results["exists"] == False and args.onlyused == False:
            websiteprint = print_color("[-] " + results["domain"], "magenta",args)
            print(websiteprint)
        elif results["exists"] == True:
            toprint = ""
            if results["emailrecovery"] is not None:
                toprint += " " + results["emailrecovery"]
            if results["phoneNumber"] is not None:
                toprint += " / " + results["phoneNumber"]
            if results["others"] is not None and "FullName" in str(results["others"].keys()):
                toprint += " / FullName " + results["others"]["FullName"]
            if results["others"] is not None and "Date, time of the creation" in str(results["others"].keys()):
                toprint += " / Date, time of the creation " + results["others"]["Date, time of the creation"]

            websiteprint = print_color("[+] " + results["domain"] + toprint, "green",args)
            print(websiteprint)

    print("\n" + description)
    print(str(len(websites)) + " websites checked in " +
          str(round(time.time() - start_time, 2)) + " seconds")


def export_csv(data,args,email):
    """Export result to csv"""
    if args.csvoutput == True:
        now = datetime.now()
        timestamp = datetime.timestamp(now)
        name_file="holehe_"+str(round(timestamp))+"_"+email+"_results.csv"
        with open(name_file, 'w', encoding='utf8', newline='') as output_file:
            fc = csv.DictWriter(output_file,fieldnames=data[0].keys())
            fc.writeheader()
            fc.writerows(data)
        exit("All results have been exported to "+name_file)


def export_batch_csv(results, args):
    """Export all batch results to one CSV containing an email column."""
    if args.csvoutput == True:
        now = datetime.now()
        timestamp = datetime.timestamp(now)
        name_file = "holehe_"+str(round(timestamp))+"_batch_results.csv"
        rows = batch_csv_rows(results)
        fieldnames = ["email"]
        for row in rows:
            for field in row:
                if field not in fieldnames:
                    fieldnames.append(field)
        with open(name_file, 'w', encoding='utf8', newline='') as output_file:
            fc = csv.DictWriter(output_file, fieldnames=fieldnames)
            fc.writeheader()
            fc.writerows(rows)
        exit("All results have been exported to "+name_file)


async def launch_module(module,email, client, out, limiter=None):
    data={'aboutme': 'about.me', 'adobe': 'adobe.com', 'amazon': 'amazon.com', 'anydo': 'any.do', 'archive': 'archive.org', 'armurerieauxerre': 'armurerie-auxerre.com', 'atlassian': 'atlassian.com', 'babeshows': 'babeshows.co.uk', 'badeggsonline': 'badeggsonline.com', 'biosmods': 'bios-mods.com', 'biotechnologyforums': 'biotechnologyforums.com', 'bitmoji': 'bitmoji.com', 'blablacar': 'blablacar.com', 'blackworldforum': 'blackworldforum.com', 'blip': 'blip.fm', 'blitzortung': 'forum.blitzortung.org', 'bluegrassrivals': 'bluegrassrivals.com', 'bodybuilding': 'bodybuilding.com', 'buymeacoffee': 'buymeacoffee.com', 'cambridgemt': 'discussion.cambridge-mt.com', 'caringbridge': 'caringbridge.org', 'chinaphonearena': 'chinaphonearena.com', 'clashfarmer': 'clashfarmer.com', 'codecademy': 'codecademy.com', 'codeigniter': 'forum.codeigniter.com', 'codepen': 'codepen.io', 'coroflot': 'coroflot.com', 'cpaelites': 'cpaelites.com', 'cpahero': 'cpahero.com', 'cracked_to': 'cracked.to', 'crevado': 'crevado.com', 'deliveroo': 'deliveroo.com', 'demonforums': 'demonforums.net', 'devrant': 'devrant.com', 'diigo': 'diigo.com', 'discord': 'discord.com', 'docker': 'docker.com', 'dominosfr': 'dominos.fr', 'ebay': 'ebay.com', 'ello': 'ello.co', 'envato': 'envato.com', 'eventbrite': 'eventbrite.com', 'evernote': 'evernote.com', 'fanpop': 'fanpop.com', 'firefox': 'firefox.com', 'flickr': 'flickr.com', 'freelancer': 'freelancer.com', 'freiberg': 'drachenhort.user.stunet.tu-freiberg.de', 'garmin': 'garmin.com', 'github': 'github.com', 'google': 'google.com', 'gravatar': 'gravatar.com', 'imgur': 'imgur.com', 'instagram': 'instagram.com', 'issuu': 'issuu.com', 'koditv': 'forum.kodi.tv', 'komoot': 'komoot.com', 'laposte': 'laposte.fr', 'lastfm': 'last.fm', 'lastpass': 'lastpass.com', 'mail_ru': 'mail.ru', 'mybb': 'community.mybb.com', 'myspace': 'myspace.com', 'nattyornot': 'nattyornotforum.nattyornot.com', 'naturabuy': 'naturabuy.fr', 'ndemiccreations': 'forum.ndemiccreations.com', 'nextpvr': 'forums.nextpvr.com', 'nike': 'nike.com', 'odnoklassniki': 'ok.ru', 'office365': 'office365.com', 'onlinesequencer': 'onlinesequencer.net', 'parler': 'parler.com', 'patreon': 'patreon.com', 'pinterest': 'pinterest.com', 'plurk': 'plurk.com', 'pornhub': 'pornhub.com', 'protonmail': 'protonmail.ch', 'quora': 'quora.com', 'rambler': 'rambler.ru', 'redtube': 'redtube.com', 'replit': 'replit.com', 'rocketreach': 'rocketreach.co', 'samsung': 'samsung.com', 'seoclerks': 'seoclerks.com', 'sevencups': '7cups.com', 'smule': 'smule.com', 'snapchat': 'snapchat.com', 'soundcloud': 'soundcloud.com', 'sporcle': 'sporcle.com', 'spotify': 'spotify.com', 'strava': 'strava.com', 'taringa': 'taringa.net', 'teamtreehouse': 'teamtreehouse.com', 'tellonym': 'tellonym.me', 'thecardboard': 'thecardboard.org', 'therianguide': 'forums.therian-guide.com', 'thevapingforum': 'thevapingforum.com', 'tumblr': 'tumblr.com', 'tunefind': 'tunefind.com', 'twitter': 'twitter.com', 'venmo': 'venmo.com', 'vivino': 'vivino.com', 'voxmedia': 'voxmedia.com', 'vrbo': 'vrbo.com', 'vsco': 'vsco.co', 'wattpad': 'wattpad.com', 'wordpress': 'wordpress.com', 'xing': 'xing.com', 'xnxx': 'xnxx.com', 'xvideos': 'xvideos.com', 'yahoo': 'yahoo.com','hubspot': 'hubspot.com', 'pipedrive': 'pipedrive.com', 'insightly': 'insightly.com', 'nutshell': 'nutshell.com', 'zoho': 'zoho.com', 'axonaut': 'axonaut.com', 'amocrm': 'amocrm.com', 'nimble': 'nimble.com', 'nocrm': 'nocrm.io', 'teamleader': 'teamleader.eu'}
    try:
        if limiter is None:
            await module(email, client, out)
        else:
            async with limiter:
                await module(email, client, out)
    except Exception:
        name = getattr(module, "__name__", str(module))
        if name not in data:
            name = str(module).split('<function ')[1].split(' ')[0]
        out.append({"name": name,"domain":data.get(name, name),
                    "rateLimit": False,
                    "error": True,
                    "exists": False,
                    "emailrecovery": None,
                    "phoneNumber": None,
                    "others": None})


async def run_batch(emails, websites, client, concurrency=DEFAULT_CONCURRENCY):
    """Check every email, bounding concurrent site modules globally."""
    limiter = trio.CapacityLimiter(concurrency)
    results = [None] * len(emails)

    async def check_email(index, email):
        out = []
        async with trio.open_nursery() as nursery:
            for website in websites:
                nursery.start_soon(
                    launch_module,
                    website,
                    email,
                    client,
                    out,
                    limiter,
                )
        out = deduplicate_results(out)
        results[index] = (
            email,
            sorted(out, key=lambda item: (item.get("name") or "", item.get("domain") or "")),
        )

    async with trio.open_nursery() as nursery:
        for index, email in enumerate(emails):
            nursery.start_soon(check_email, index, email)

    return results


def build_parser():
    parser= ArgumentParser(description=f"holehe v{__version__}")
    parser.add_argument("email",
                    nargs='*', metavar='EMAIL',
                    help="Target Email (multiple emails are checked as a batch)")
    parser.add_argument("-f", "--email-file", "--email-list", action="append", default=[], dest="email_file",
                    help="Read target emails from a file, one per line (repeatable)")
    parser.add_argument("-j", "--concurrency", "--max-concurrency", "--workers", type=positive_int,
                    default=DEFAULT_CONCURRENCY, dest="concurrency",
                    help=f"Maximum concurrently running site modules (default {DEFAULT_CONCURRENCY})")
    parser.add_argument("--rate-limit", "--rps", "--requests-per-second", type=non_negative_float,
                    default=DEFAULT_RATE_LIMIT, dest="rate_limit",
                    help=f"Maximum HTTP requests per second; 0 disables the limit (default {DEFAULT_RATE_LIMIT:g})")
    parser.add_argument("--only-used", default=False, required=False,action="store_true",dest="onlyused",
                    help="Displays only the sites used by the target email address.")
    parser.add_argument("--no-color", default=False, required=False,action="store_true",dest="nocolor",
                    help="Don't color terminal output")
    parser.add_argument("--no-clear", default=False, required=False,action="store_true",dest="noclear",
                    help="Do not clear the terminal to display the results")
    parser.add_argument("-NP","--no-password-recovery", default=False, required=False,action="store_true",dest="nopasswordrecovery",
                    help="Do not try password recovery on the websites")
    parser.add_argument("-C","--csv", default=False, required=False,action="store_true",dest="csvoutput",
                    help="Create a CSV with the results")
    parser.add_argument("-T","--timeout", type=int , default=10, required=False,dest="timeout",
                    help="Set max timeout value (default 10)")
    parser.add_argument("--no-update", default=False, required=False, action="store_true", dest="noupdate",
                    help="Do not check PyPI for a newer Holehe version")
    return parser


def print_batch_results(results, args, websites, start_time):
    """Print one legacy-compatible block per email."""
    if len(results) == 1:
        email, data = results[0]
        print_result(data, args, email, start_time, websites)
        return

    batch_args = copy.copy(args)
    batch_args.noclear = True
    for email, data in results:
        print_result(data, batch_args, email, start_time, websites)


async def maincore():
    parser = build_parser()
    args = parser.parse_args()
    if not args.noupdate:
        check_update()
    credit()
    emails = validate_emails(collect_emails(args))

    # Import Modules
    modules = import_submodules("holehe.modules")
    websites = get_functions(modules,args)
    # Get timeout
    timeout=args.timeout
    # Start time
    start_time = time.time()
    # Def the async client
    client = RateLimitedAsyncClient(timeout=timeout, rate_limit=args.rate_limit)
    # Launching the modules
    instrument = TrioProgress(len(websites) * len(emails))
    trio.lowlevel.add_instrument(instrument)
    try:
        results = await run_batch(
            emails,
            websites,
            client,
            concurrency=args.concurrency,
        )
    finally:
        trio.lowlevel.remove_instrument(instrument)
        instrument.close()
        await client.aclose()
    # Print the results
    print_batch_results(results, args, websites, start_time)
    credit()
    # Export results
    if len(results) == 1:
        email, out = results[0]
        export_csv(out,args,email)
    else:
        export_batch_csv(results, args)

def main():
    trio.run(maincore)
