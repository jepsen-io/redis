from util import (
    recover,
    save_error_log,
    start_eloqkv_cluster,
    run_jepsen_test,
    check_client_num,
    flushdb,
    check_stdout_log,
)

import subprocess
import sys


class CriticalError(Exception):
    """Custom exception to indicate a critical error that should terminate the loop."""

    pass


def handle_error():
    """Handles errors by saving the error log and terminating specific processes."""
    save_error_log()
    try:
        subprocess.run(
            'pkill -f "memtier.sh|memtier_benchmark"', shell=True, check=True
        )
        print("Successfully executed pkill command.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while executing pkill: {e}")
    raise CriticalError("Terminating loop due to critical error.")


def main():
    try:
        for i in range(1, 3000):
            print(f"Iteration {i} started.")
            recover()
            # Uncomment the following lines if needed
            # start_eloqkv_cluster()
            # flushdb()

            # Check for crashes in eloqkv during recover
            if not check_stdout_log():
                print("Crash detected in eloqkv during recover.")
                handle_error()

            # Run Jepsen test and check for crashes
            jepsen_result = run_jepsen_test()
            if jepsen_result != 0:
                print("Crash detected during Jepsen test.")
                handle_error()

            # Uncomment if you need to check the number of clients
            # check_client_num()

            # Check for crashes in eloqkv during Jepsen test
            if not check_stdout_log():
                print("Crash detected in eloqkv during Jepsen test.")
                handle_error()

            print(f"Iteration {i} completed successfully.\n")
        else:
            print("Completed all iterations without encountering critical errors.")
    except CriticalError as e:
        print(e)
        print("Exiting the script due to a critical error.")


if __name__ == "__main__":
    main()
