import os
import queue
import re

metrics = {
    "files_processed": 0,
    "total_lines_parsed": 0,
    "total_bandwidth": 0,
    "status_code_freq": {},
    "client_ips_freq": {}
}

"""
status code and client ip is code/ip : frequency nested dictionary, will parse into ordered list later (?)

"""
def producer(root_dir: str, work_queue: queue.Queue, num_workers: int) -> None:
    """Recursively finds .log files and enqueues them with backpressure."""
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".log"):
                filepath = os.path.join(root, file)
                # Automatically blocks if the queue already holds 'q' items
                work_queue.put(filepath)

    # Send one sentinel ("poison pill") per worker so every thread knows to exit
    for _ in range(num_workers):
        work_queue.put(None)

def worker(work_queue: queue.Queue, metrics: dict, metrics_lock) -> None:
    """Continuously pulls log files from the queue and parses them."""
    while True:
        log_file = work_queue.get()
        try:
            # Check for the poison pill termination signal
            if log_file is None:
                break

            # Read and parse outside the lock to avoid slowing down other threads
            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    parseLine(line)

            # Acquire lock ONLY to update shared global metrics
            with metrics_lock:
                metrics["files_processed"] += 1

        except OSError as e:
            # Safely log unreadable/missing files without crashing the worker
            print(f"Error reading {log_file}: {e}")
        finally:
            # Must run on every .get(), including the poison pill and errors
            work_queue.task_done()

def parseLine(line): #parses line and updates metrics dict
    """Parse a single line from a log file and return relevant metrics."""
    pattern = r'\[([^\]]+)\]|"([^"]+)"|(\S+)' #regex converting str into list 
    fields = [match[0] or match[1] or match[2] for match in re.findall(pattern, line)] #the actual list of fields

def report(metrics, top_k):
    """Prints a summary of the aggregated metrics."""
    print(f"Files Processed: {metrics['files_processed']}")
    print(f"Total Lines Parsed: {metrics['total_lines_parsed']}")
    print(f"Total Bandwidth: {metrics['total_bandwidth'] / 1_000_000:.2f} MB")
    print("Status Code Breakdown:")
    for code, count in sorted(metrics["status_code_freq"].items()):
        print(f"  {code}: {count}")
    print(f"Top {top_k} Client IPs:")
    top_ips = sorted(metrics["client_ips_freq"].items(), key=lambda x: x[1], reverse=True)[:top_k]
    for ip, count in top_ips:
        print(f"  {ip}: {count}")

def main(root_dir: str, num_workers: int, top_k: int) -> None: #main function thatll be called
    pass

"""
As above, each log entry has 7 fields:

IP address of the client.
The "remote logname" (sometimes "-" if not available/provided).
The remote username (sometimes "-" if no information is available/provided).
The date and time that the request was processed by the server (always contained within square brackets []).
The first line of the HTTP request (always contained within quotations "").
The HTTP status code that was returned by the server, to the client (an integer value).
The number of payload bytes transferred. (In cases where 0 bytes are transferred, "-" is sometimes written instead of "0".)
Required Aggregated Metrics
Your application must track and output the following metrics upon completion:

Files Processed: Total count of successfully parsed .log files.
Total Lines Parsed: Total count of log lines matched and recorded.
Total Bandwidth: Sum of all payload bytes transferred (converted to Megabytes using 1 MB = 1,000,000 bytes).
Status Code Breakdown: Frequency dictionary mapping HTTP status codes to hit counts (e.g., {200: 45000, 404: 120, 500: 15}).
Top Client IPs: The top K most active IP addresses by request count.
"""