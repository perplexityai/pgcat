"""Bounded local comparison; requires the same scratch DB as integration tests."""
import subprocess
import sys

import utils
from test_registered_plugins import CONFIG, PORT


def main():
    binaries = sys.argv[1:]
    if not binaries:
        raise SystemExit("pass baseline and candidate pgcat binaries")
    # Keep parser configuration identical between baseline and candidate.
    config = CONFIG.replace("query_parser_enabled = false", "query_parser_enabled = true")
    for round_number in range(3):
        for binary in binaries:
            process = utils.PgcatProcess(config, PORT, binary=binary)
            try:
                process.start()
                for protocol in ("simple", "extended"):
                    result = subprocess.run(
                        [
                            "docker", "run", "--rm", "-i", "--network", "host",
                            "-e", "PGPASSWORD=sharding_user", "postgres:14",
                            "pgbench", "-h", "127.0.0.1", "-p", str(PORT),
                            "-U", "sharding_user", "-n", "-c", "8", "-j", "2",
                            "-T", "3", "-M", protocol, "-f", "/dev/stdin", "registration",
                        ],
                        input="SELECT 1;\n",
                        text=True,
                        capture_output=True,
                        check=True,
                    )
                    print(f"ROUND {round_number + 1} {binary} {protocol}", flush=True)
                    print(result.stdout, flush=True)
            finally:
                process.stop()


if __name__ == "__main__":
    main()
