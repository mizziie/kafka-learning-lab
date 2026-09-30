"""
Load test for the Kafka Gateway.

This script fires many concurrent POST /call requests at the gateway and
reports throughput, success rate, and latency statistics. It is useful for
understanding how the gateway + Kafka + worker behave under load.
"""

import argparse
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

DEFAULT_URL = "http://localhost:8000/call"
DEFAULT_PAYLOAD = {"action": "loadtest", "data": {"value": 1}}


def send_one(session: requests.Session, url: str, payload: dict, timeout: int):
    start = time.perf_counter()
    try:
        response = session.post(url, json=payload, timeout=timeout)
        latency = (time.perf_counter() - start) * 1000
        return response.status_code, latency, None
    except Exception as exc:
        latency = (time.perf_counter() - start) * 1000
        return None, latency, str(exc)


def main():
    parser = argparse.ArgumentParser(
        description="Load test the Kafka Gateway (POST /call)"
    )
    parser.add_argument(
        "-n", "--number", type=int, default=1000, help="total number of requests"
    )
    parser.add_argument(
        "-c", "--concurrency", type=int, default=50, help="number of concurrent workers"
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="gateway endpoint")
    parser.add_argument("--timeout", type=int, default=10, help="request timeout in seconds")
    args = parser.parse_args()

    session = requests.Session()
    payload = DEFAULT_PAYLOAD

    print(f"Target: {args.url}")
    print(f"Requests: {args.number}, Concurrency: {args.concurrency}, Timeout: {args.timeout}s")
    print("-" * 50)

    begin = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [
            executor.submit(send_one, session, args.url, payload, args.timeout)
            for _ in range(args.number)
        ]
        results = [future.result() for future in as_completed(futures)]
    duration = time.time() - begin

    statuses = [status for status, _, _ in results if status is not None]
    errors = [error for _, _, error in results if error]
    latencies = [latency for _, latency, _ in results]

    success = sum(1 for status in statuses if status == 200)
    failed = len(statuses) - success + len(errors)

    print(f"Duration:    {duration:.2f} s")
    print(f"Total:       {args.number}")
    print(f"Success:     {success}")
    print(f"Failed:      {failed}")
    print(f"RPS:         {args.number / duration:.2f}")

    if latencies:
        print(f"Min latency: {min(latencies):.2f} ms")
        print(f"Avg latency: {statistics.mean(latencies):.2f} ms")
        print(f"Max latency: {max(latencies):.2f} ms")
        if len(latencies) > 1:
            p95 = statistics.quantiles(latencies, n=20)[18]
            print(f"P95 latency: {p95:.2f} ms")

    print("-" * 50)
    print("Tip: watch consumer lag in Kafka UI at http://localhost:8081")


if __name__ == "__main__":
    main()
