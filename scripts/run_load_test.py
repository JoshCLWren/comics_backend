#!/usr/bin/env python
"""Utility to ensure the API is running before executing the load test."""

from __future__ import annotations

import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import IO


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the load test, launching the API server if needed."
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="API host (default: %(default)s)"
    )
    parser.add_argument(
        "--port", type=int, default=8000, help="API port (default: %(default)s)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Rows to replay (use 0 to process the entire CSV; default: %(default)s)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Concurrent requests during load test (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout for load test requests (default: %(default)s seconds)",
    )
    parser.add_argument(
        "--app-module",
        default="main:app",
        help="Uvicorn app module to launch when needed (default: %(default)s)",
    )
    return parser.parse_args()


def server_running(host: str, port: int) -> bool:
    with socket.socket() as sock:
        sock.settimeout(1)
        return sock.connect_ex((host, port)) == 0


def wait_for_server(host: str, port: int, timeout: float = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if server_running(host, port):
            return
        time.sleep(0.5)
    raise RuntimeError(f"API failed to start on {host}:{port} within {timeout} seconds")


def run_load_test(base_url: str, limit: int, concurrency: int, timeout: float) -> None:
    env = os.environ.copy()
    csv_path = (Path("data") / "clz_export.csv").resolve()
    env.setdefault("LOAD_TEST_CSV_PATH", str(csv_path))
    env["LOAD_TEST_TIMEOUT"] = str(timeout)
    if limit > 0:
        env["LOAD_TEST_ROW_LIMIT"] = str(limit)
    else:
        env.pop("LOAD_TEST_ROW_LIMIT", None)
    cmd = [
        sys.executable,
        "-m",
        "locust",
        "--headless",
        "-f",
        str(Path("scripts") / "load_test.py"),
        "--host",
        base_url,
        "-u",
        str(max(1, concurrency)),
        "-r",
        str(max(1, concurrency)),
    ]
    subprocess.run(cmd, check=True, env=env)


def launch_api(
    app_module: str, host: str, port: int
) -> tuple[subprocess.Popen[bytes], IO[bytes]]:
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        app_module,
        "--host",
        host,
        "--port",
        str(port),
        "--log-level",
        "warning",
    ]
    log_path = Path("/tmp/load-test-api.log")
    log_file = log_path.open("wb")
    print(f"Starting temporary API server on {host}:{port} (logs -> {log_path})")
    proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
    return proc, log_file


def terminate_process(
    proc: subprocess.Popen[bytes], log_file: IO[bytes] | None
) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    if log_file:
        log_file.close()


def main() -> None:
    args = parse_args()
    base_url = f"http://{args.host}:{args.port}"
    server_proc: subprocess.Popen[bytes] | None = None
    log_file: IO[bytes] | None = None
    reused = server_running(args.host, args.port)

    if reused:
        print(f"Detected existing API at {base_url}; reusing server")
    else:
        server_proc, log_file = launch_api(args.app_module, args.host, args.port)
        try:
            wait_for_server(args.host, args.port)
        except Exception:
            terminate_process(server_proc, log_file)
            raise

    try:
        run_load_test(base_url, args.limit, args.concurrency, args.timeout)
    finally:
        if server_proc:
            print("Shutting down temporary API server")
            terminate_process(server_proc, log_file)


if __name__ == "__main__":
    main()
