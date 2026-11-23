def producer(d, n, q, k):
    """
    d = directory to search for log files
    n = number of workers
    q = length of queue
    k = number of active ip addresses to output in summary
    """
    import os
    log_files = [] #queue of length q
    for root, dirs, files in os.walk(d):
        for file in files:
            if file.endswith('.log'):
                    if len(log_files) < q:
                        log_files.append(os.path.join(root, file))
                    else:
                         pass #await until queue is not full
    return log_files


def worker(log_file):
    """
    Processes a single log file.
    
    """
    with open(log_file, 'r') as f:
        pass