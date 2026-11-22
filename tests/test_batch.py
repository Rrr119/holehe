import os
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import httpx
import trio
from trio.testing import MockClock

from holehe import core


def result_record(name="example", domain="example.com"):
    return {
        "name": name,
        "domain": domain,
        "method": "register",
        "frequent_rate_limit": False,
        "rateLimit": False,
        "exists": True,
        "emailrecovery": None,
        "phoneNumber": None,
        "others": None,
    }


class BatchLookupTests(unittest.TestCase):
    def test_100_emails_are_limited_and_deduplicated(self):
        raw_emails = [
            f"user{i:03d}@example.com" for i in range(100)
        ]
        raw_emails.extend(
            [
                "USER000@example.com",
                "user001@example.com ",
                "user000@example.com",
            ]
        )
        emails = core.deduplicate_emails(raw_emails)
        self.assertEqual(len(emails), 100)
        self.assertEqual(emails[0], "user000@example.com")

        calls = []
        active = 0
        max_active = 0

        async def fake_module(email, client, out):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            calls.append(email)
            await trio.sleep(0.005)
            out.append(result_record())
            out.append(result_record())
            active -= 1

        async def scenario():
            return await core.run_batch(
                emails,
                [fake_module],
                client=None,
                concurrency=7,
            )

        results = trio.run(scenario)

        self.assertEqual(len(calls), 100)
        self.assertEqual(sorted(calls), sorted(emails))
        self.assertLessEqual(max_active, 7)
        self.assertEqual([email for email, _ in results], emails)
        self.assertTrue(all(len(data) == 1 for _, data in results))
        self.assertEqual(
            set(results[0][1][0]),
            set(result_record()),
        )

    def test_100_emails_from_file_and_cli_options(self):
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".txt", delete=False
        ) as email_file:
            email_file.write("# batch targets\n")
            for index in range(100):
                email_file.write(f"file{index:03d}@example.com\n")
            path = email_file.name

        try:
            parser = core.build_parser()
            args = parser.parse_args(
                ["-f", path, "--concurrency", "4", "--rate-limit", "12.5"]
            )
            emails = core.validate_emails(core.collect_emails(args))
        finally:
            os.unlink(path)

        self.assertEqual(len(emails), 100)
        self.assertEqual(args.concurrency, 4)
        self.assertEqual(args.rate_limit, 12.5)

    def test_duplicate_site_results_prefer_existing_account(self):
        limited = result_record()
        limited.update({"rateLimit": True, "exists": False})
        found = result_record()
        found["others"] = {"FullName": "Example"}

        deduplicated = core.deduplicate_results([limited, found, found])

        self.assertEqual(len(deduplicated), 1)
        self.assertTrue(deduplicated[0]["exists"])
        self.assertEqual(
            deduplicated[0]["others"],
            {"FullName": "Example"},
        )

    def test_batch_csv_rows_keep_legacy_fields(self):
        payload = [("user@example.com", [result_record()])]
        rows = core.batch_csv_rows(payload)

        self.assertEqual(rows[0]["email"], "user@example.com")
        self.assertEqual(rows[0]["name"], "example")
        self.assertEqual(rows[0]["domain"], "example.com")
        self.assertIn("emailrecovery", rows[0])
        self.assertIn("phoneNumber", rows[0])

    def test_single_email_output_is_unchanged(self):
        parser = core.build_parser()
        args = parser.parse_args(["--no-color", "--no-clear", "user@example.com"])
        output = []

        def capture(value, *args, **kwargs):
            output.append(value)

        with patch("builtins.print", capture):
            core.print_batch_results(
                [("user@example.com", [result_record()])],
                args,
                [lambda: None],
                time.time(),
            )

        rendered = "\n".join(output)
        self.assertIn("   user@example.com", rendered)
        self.assertIn("[+] example.com", rendered)
        self.assertIn("1 websites checked in", rendered)

    def test_maincore_processes_100_emails_without_network(self):
        emails = [f"cli{i:03d}@example.com" for i in range(100)]
        captured = []

        async def fake_module(email, client, out):
            await trio.sleep(0)
            out.append(result_record())
            out.append(result_record())

        class DummyProgress(trio.abc.Instrument):
            def __init__(self, total):
                self.total = total

            def close(self):
                pass

        def capture_results(results, args, websites, start_time):
            captured.extend(results)

        argv = ["holehe", "--no-update", *emails]
        with patch.object(sys, "argv", argv), patch.object(
            core, "credit"
        ), patch.object(
            core, "import_submodules", return_value={}
        ), patch.object(
            core, "get_functions", return_value=[fake_module]
        ), patch.object(
            core, "TrioProgress", DummyProgress
        ), patch.object(
            core, "print_batch_results", capture_results
        ):
            trio.run(core.maincore)

        self.assertEqual(len(captured), 100)
        self.assertEqual([email for email, _ in captured], emails)
        self.assertTrue(all(len(data) == 1 for _, data in captured))


class RateLimiterTests(unittest.TestCase):
    def test_requests_are_spaced_by_rate_limit(self):
        async def scenario():
            limiter = core.AsyncRateLimiter(10)
            start = trio.current_time()
            for _ in range(4):
                await limiter.acquire()
            return trio.current_time() - start

        elapsed = trio.run(
            scenario,
            clock=MockClock(autojump_threshold=0),
        )
        self.assertAlmostEqual(elapsed, 0.3, places=6)

    def test_zero_rate_limit_disables_pacing(self):
        async def scenario():
            limiter = core.AsyncRateLimiter(0)
            start = trio.current_time()
            for _ in range(10):
                await limiter.acquire()
            return trio.current_time() - start

        elapsed = trio.run(
            scenario,
            clock=MockClock(autojump_threshold=0),
        )
        self.assertEqual(elapsed, 0)

    def test_redirect_hops_are_rate_limited(self):
        urls = []

        def handler(request):
            urls.append(str(request.url))
            if request.url.path == "/start":
                return httpx.Response(
                    302,
                    headers={"location": "https://example.com/finish"},
                )
            return httpx.Response(200, text="ok")

        async def scenario():
            transport = httpx.MockTransport(handler)
            client = core.RateLimitedAsyncClient(
                transport=transport,
                rate_limit=10,
                follow_redirects=True,
            )
            start = trio.current_time()
            async with client:
                response = await client.get("https://example.com/start")
            return response, trio.current_time() - start

        response, elapsed = trio.run(
            scenario,
            clock=MockClock(autojump_threshold=0),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            urls,
            [
                "https://example.com/start",
                "https://example.com/finish",
            ],
        )
        self.assertAlmostEqual(elapsed, 0.1, places=6)


if __name__ == "__main__":
    unittest.main()
