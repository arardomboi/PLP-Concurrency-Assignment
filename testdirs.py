import os
import random

TARGET_BYTES = 5_000_000  # ~5 MB total across nested files
NUM_FILES = 20            # Spread across nested subdirectories to test os.walk()
OUTPUT_DIR = "testdir"

# Weighted pools so Top-K IPs and Status Code Breakdown look realistic
IPS = [
    "194.4.56.49", "194.4.56.49", "194.4.56.49", "194.4.56.49",  # High-volume IP
    "171.61.252.233", "171.61.252.233", "171.61.252.233",
    "164.103.195.193", "164.103.195.193",
    "192.168.1.10", "10.0.0.42", "172.16.0.8", "203.0.113.19", "198.51.100.77",
]
ENDPOINTS = [
    "GET /api/v1/resource HTTP/1.1",
    "POST /api/v1/auth/login HTTP/1.1",
    "GET /static/app.js HTTP/1.1",
    "GET /apache_pb.gif HTTP/1.0",
    "DELETE /api/v1/session HTTP/1.1",
]
STATUS_CODES = [200, 200, 200, 200, 301, 404, 500]
USERS = ["-", "-", "-", "john", "admin", "service-acct"]


def generate_logs() -> None:
    """Creates a nested directory tree containing ~5 MB of Common Logfile data."""
    bytes_written = 0
    lines_written = 0
    bytes_per_file = TARGET_BYTES // NUM_FILES

    for i in range(NUM_FILES):
        # Create nested paths like testdir/service_0/node_1/access_0.log
        nested_dir = os.path.join(OUTPUT_DIR, f"service_{i % 4}", f"node_{i % 3}")
        os.makedirs(nested_dir, exist_ok=True)
        filepath = os.path.join(nested_dir, f"access_{i}.log")

        file_bytes = 0
        with open(filepath, "w", encoding="utf-8") as f:
            while file_bytes < bytes_per_file:
                # Inject a malformed line every ~5,000 lines to test error resilience
                if lines_written > 0 and lines_written % 5000 == 0:
                    bad_line = "CORRUPT_ENTRY missing brackets and status\n"
                    f.write(bad_line)
                    file_bytes += len(bad_line)
                    lines_written += 1
                    continue

                ip = random.choice(IPS)
                user = random.choice(USERS)
                endpoint = random.choice(ENDPOINTS)
                status = random.choice(STATUS_CODES)
                # Include '-' for 0-byte payloads occasionally per brief spec
                payload = "-" if random.random() < 0.05 else str(random.randint(128, 16384))

                line = f'{ip} - {user} [30/Jul/2026:12:00:00 +0000] "{endpoint}" {status} {payload}\n'
                f.write(line)
                file_bytes += len(line)
                lines_written += 1

        bytes_written += file_bytes

    # Add an ignored non-.log file to verify producer extension filtering
    with open(os.path.join(OUTPUT_DIR, "README.txt"), "w", encoding="utf-8") as f:
        f.write("This file should be ignored by the producer.\n")

    print(f"Generated {NUM_FILES} .log files in '{OUTPUT_DIR}/' ({bytes_written / 1_000_000:.2f} MB)")


if __name__ == "__main__":
    generate_logs()
