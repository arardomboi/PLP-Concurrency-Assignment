#imports

import os
import queue
import re
import sys
import threading
import time
from collections import Counter


"""
0 = ip
1 = remote logname
2 = remote username
3 = timestamp
4 = request
5 = status code
6 = payload bytes

"""
#regex
LOG_PATTERN = re.compile(
    r'^(\S+)\s+(\S+)\s+(\S+)\s+\[([^\]]+)\]\s+"([^"]*)"\s+(\d{3})\s+(\d+|-)$'
)


def parse_args() -> tuple[str, int, int, int]:
    """
    Parse and validate the four required positional CLI arguments.

    Expected invocation:
        python main.py <d> <n> <q> <k>

    Returns:
        tuple[str, int, int, int]: (root_dir, num_workers, max_queue_size, top_k)
    """
    if len(sys.argv) != 5:
        print("Usage: python main.py <d> <n> <q> <k>")
        sys.exit(1)

    root_dir = sys.argv[1]
    if not os.path.isdir(root_dir):
        print(f"Error: Directory '{root_dir}' does not exist.")
        sys.exit(1)

    try:
        num_workers = int(sys.argv[2])
        max_queue_size = int(sys.argv[3])
        top_k = int(sys.argv[4])
    except ValueError:
        print("Error: n, q, and k must be integers.")
        sys.exit(1)

    if num_workers <= 0 or max_queue_size <= 0 or top_k <= 0:
        print("Error: n, q, and k must be positive integers.")
        sys.exit(1)

    return root_dir, num_workers, max_queue_size, top_k


def parse_line(line: str) -> tuple[str, int, int] | None:
    """
    Parse a single Common Logfile Format entry into structured values.

    Args:
        line (str): Raw line read from a .log file.

    Returns:
        tuple[str, int, int] | None: (ip_address, status_code, bytes_transferred),
        or None if the line is malformed or unparseable.
    """
    match = LOG_PATTERN.match(line.strip())
    if not match:
        return None

    ip_address = match.group(1)
    status_code = int(match.group(6))

    # In Common Logfile Format, '-' indicates 0 payload bytes transferred
    raw_bytes = match.group(7)
    bandwidth = int(raw_bytes) if raw_bytes != "-" else 0

    return ip_address, status_code, bandwidth #returns usefal data


def producer(root_dir: str, work_queue: queue.Queue, num_workers: int) -> None:
    """
    Producer thread target: recursively discover .log files and enqueue them.

    Blocks automatically when the bounded work_queue reaches maxsize, providing
    natural backpressure so memory usage remains bounded. Once directory
    traversal finishes, pushes one None sentinel ("poison pill") per worker
    to signal clean shutdown.

    Args:
        root_dir (str): Root directory path to traverse.
        work_queue (queue.Queue): Thread-safe bounded queue for file paths.
        num_workers (int): Number of active consumer worker threads.
    """
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".log"):
                filepath = os.path.join(root, file)
                # Blocks if queue size == q until a consumer removes an item
                work_queue.put(filepath)

    # Emit one poison pill per worker so every consumer knows no more work will arrive
    for _ in range(num_workers):
        work_queue.put(None)


def worker(
    work_queue: queue.Queue,
    metrics: dict,
    metrics_lock: threading.Lock,
) -> None:
    """
    Consumer thread target: pull file paths from the queue, parse lines, and
    safely aggregate metrics into the shared global state.

    Minimizes lock contention by performing all file I/O and regex parsing
    into thread-local accumulators, acquiring metrics_lock only once per file.

    Args:
        work_queue (queue.Queue): Shared bounded queue holding file paths.
        metrics (dict): Shared dictionary storing global aggregated metrics.
        metrics_lock (threading.Lock): Mutex lock protecting metrics updates.
    """
    while True:
        log_file = work_queue.get()
        try:
            # Check for poison pill shutdown signal
            if log_file is None:
                break

            # Thread-local counters to avoid holding the mutex lock during I/O
            local_lines = 0
            local_bandwidth = 0
            local_statuses = Counter()
            local_ips = Counter()

            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    parsed = parse_line(line)
                    if parsed is None:
                        # Skip corrupt/unparseable lines without crashing
                        continue

                    ip_address, status_code, bandwidth = parsed
                    local_lines += 1
                    local_bandwidth += bandwidth
                    local_statuses[status_code] += 1
                    local_ips[ip_address] += 1

            # Critical section: merge local results into shared global metrics
            with metrics_lock:
                metrics["files_processed"] += 1
                metrics["total_lines_parsed"] += local_lines
                metrics["total_bandwidth"] += local_bandwidth
                metrics["status_code_freq"].update(local_statuses)
                metrics["client_ips_freq"].update(local_ips)

        except (OSError, UnicodeDecodeError) as e:
            # Log unreadable/corrupt files safely without terminating the worker
            print(f"Error reading {log_file}: {e}")
        finally:
            # Always mark task done (even on errors or poison pills) to prevent deadlocks
            work_queue.task_done()


def print_summary(
    elapsed_time: float,
    num_workers: int,
    max_queue_size: int,
    metrics: dict,
    top_k: int,
) -> None:
    """
    Format and print the final execution and log metrics report to stdout.

    Follows exact formatting rules:
      - 50-character horizontal rules ('=')
      - Centered header and aligned colons
      - 2 decimal places for floats, 0 for integers, comma thousands separators
        (except inside the status code frequency dictionary)
      - 1 MB = 1,000,000 bytes conversion

    Args:
        elapsed_time (float): Total wall-clock runtime in seconds.
        num_workers (int): Number of worker threads used (n).
        max_queue_size (int): Maximum bounded queue capacity (q).
        metrics (dict): Final aggregated metrics dictionary.
        top_k (int): Number of top client IPs to display (k).
    """
    border = "=" * 50
    border_line = "-" * 50
    total_mb = metrics["total_bandwidth"] / 1_000_000

    # Format status codes sorted ascending without comma separators on counts
    sorted_statuses = sorted(metrics["status_code_freq"].items())
    status_str = ", ".join(f"{code}: {count}" for code, count in sorted_statuses)

    print(border)
    print(f"{'LOG ANALYSIS COMPLETE':^50}")
    print(border)
    print(f"{'Execution Time':<21}: {elapsed_time:.2f} seconds")
    print(f"{'Worker Threads Used':<21}: {num_workers}")
    print(f"{'Max Queue Capacity':<21}: {max_queue_size}")
    print(border_line)
    print(f"{'Files Processed':<21}: {metrics['files_processed']:,}")
    print(f"{'Total Lines Parsed':<21}: {metrics['total_lines_parsed']:,}")
    print(f"{'Total Bandwidth':<21}: {total_mb:,.2f} MB")
    print(f"{'Status Code Frequency':<21}: {{{status_str}}}")
    print(f"{f'Top {top_k} Client IPs':<21}:")

    top_ips = metrics["client_ips_freq"].most_common(top_k)
    for rank, (ip_address, count) in enumerate(top_ips, start=1):
        print(f"  {rank}. {ip_address:<15} - {count:>10,} requests")

    print(border)


def main() -> None:
    """
    Main entry point
    """
    root_dir, num_workers, max_queue_size, top_k = parse_args() #gets the inputted args

    #inits variables
    work_queue = queue.Queue(maxsize=max_queue_size)
    metrics_lock = threading.Lock()
    metrics = {
        "files_processed": 0,
        "total_lines_parsed": 0,
        "total_bandwidth": 0,
        "status_code_freq": Counter(),
        "client_ips_freq": Counter(),
    }

    #starts timer
    start_time = time.perf_counter()

    #lookfor .log files then add to queue
    producer_thread = threading.Thread(
        target=producer,
        args=(root_dir, work_queue, num_workers),
        name="ProducerThread",
    )
    producer_thread.start()

    #start worker threads
    worker_threads = []
    for i in range(num_workers):
        thread = threading.Thread(
            target=worker,
            args=(work_queue, metrics, metrics_lock),
            name=f"WorkerThread-{i + 1}",
        )
        thread.start()
        worker_threads.append(thread)

    #wait for producer and workers to finish
    producer_thread.join()
    work_queue.join()
    for thread in worker_threads:
        thread.join()

    #end timer
    elapsed_time = time.perf_counter() - start_time

    #print report
    print_summary(elapsed_time, num_workers, max_queue_size, metrics, top_k)


if __name__ == "__main__": #yea
    main()