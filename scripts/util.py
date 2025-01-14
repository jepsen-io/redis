import re
import subprocess
import sys
import os
from datetime import datetime
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()


def setup_logging():
    # Create a logger
    logger = logging.getLogger("run_and_check")
    logger.setLevel(logging.DEBUG)

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Create a file handler
    file_handler = logging.FileHandler("run_and_check.log")
    file_handler.setLevel(logging.DEBUG)

    # Create a log formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


logger = setup_logging()


# Function to run a command and capture its output
def run_command(command, timeout=None):
    logger.info(f"run command {command}")
    result = subprocess.run(
        command, shell=True, capture_output=True, text=True, check=True, timeout=timeout
    )
    return result.stdout, result.returncode


def recover():
    # Run the status command and capture its output
    try:
        status_output, _ = run_command("eloqctl status eloqkv-cluster")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get cluster status: {e}")
        return

    # Split the output into blocks for each host
    blocks = status_output.strip().split("---------------------------")

    running_nodes = {}
    dead_nodes = []

    for block in blocks:
        if not block.strip():
            continue  # Skip empty blocks

        # Extract host IP
        host_match = re.search(r"host=([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)", block)
        if not host_match:
            logger.warning(f"Could not find host in block:\n{block}")
            continue
        host = host_match.group(1)

        # Check if service is running by looking for pid
        pid_match = re.search(r"pid:\s*(\d+)", block)
        if pid_match:
            pid = int(pid_match.group(1))
            running_nodes[host] = pid
            logger.info(f"Host {host} is running with PID {pid}")
        else:
            # Service is down
            dead_nodes.append(host)
            logger.info(f"Host {host} is down")

    # Handle running nodes: reset iptables and resume process
    for host, pid in running_nodes.items():
        logger.info(f"Handling running host: {host}, PID: {pid}")
        try:
            # Reset iptables
            run_command(f"ssh {host} 'sudo iptables -F'")
            logger.info(f"Iptables reset on {host}")

            # Resume the process
            run_command(f"ssh {host} 'kill -CONT {pid}'")
            logger.info(f"Process {pid} on {host} resumed")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to execute command on {host}: {e}")

    # Handle dead nodes: start the service
    if dead_nodes:
        nodes_to_start = ",".join([f"{host}:6389" for host in dead_nodes])
        start_command = f"eloqctl start --nodes {nodes_to_start} eloqkv-cluster"
        logger.info(f"Starting dead nodes with command: {start_command}")
        try:
            start_output, _ = run_command(start_command)
            logger.info(f"Start command output: {start_output}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start dead nodes: {e}")
    else:
        logger.info("No dead nodes detected. No need to start any services.")


def start_eloqkv_cluster():
    run_command("eloqctl start eloqkv-cluster")


def run_jepsen_test():
    try:
        with open("scripts/jepsen_cmd.sh", "r") as file:
            content = file.read()
            logger.info(f"jepsen command: {content}")

        _, return_code = run_command("bash scripts/jepsen_cmd.sh")
        logger.info(f"jepsen return code:{return_code}")

        if return_code != 0 and not check_error_in_jepsen_log(
            "./store/current/jepsen.log"
        ):
            logger.info(f"Detect error caused by jepsen itself.")
            return 0
        else:
            return return_code

    except subprocess.CalledProcessError as e:
        logger.warning(f"Error occurred: {e.stderr}")
        return 1


# Define the command and arguments
redis_command = "redis-cli -h {node} -p 6389 {command}"
node_list = ["store-4", "store-5", "store-6"]
log_node = "store-8"
rsync_command = "rsync -azL '{source_dir}' '{destination_dir}'"
rsync_remote_command = "rsync -azL -e ssh eloq@{node}:{source_dir} {destination_dir}"
flushdb_command = redis_command.format(node=node_list[0], command="flushdb")
# rsync_super_server_command = "rsync -azL -e ssh error_log eloq@compute-1:/home/eloq/workspace/jepsen-eloqkv/.vscode/log"


def save_error_log():
    jepsen_source_dir = run_command("readlink -f store/current")[0].strip()
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    root_dir = f"error_log/{current_time}"
    os.makedirs(root_dir, exist_ok=True)

    # rsync
    jepsen_destination_dir = os.path.join(root_dir, "jepsen")
    run_command(
        rsync_command.format(
            source_dir=jepsen_source_dir + "/", destination_dir=jepsen_destination_dir
        )
    )
    for node in node_list:
        node_destination_dir = os.path.join(root_dir, node)
        os.makedirs(node_destination_dir, exist_ok=True)
        run_command(
            rsync_remote_command.format(
                node=node,
                source_dir="~/eloqkv-cluster/EloqKV/logs/tx-6389/eloqkv.log.INFO",
                destination_dir=os.path.join(node_destination_dir, "eloqkv.log.INFO"),
            )
        )
        run_command(
            rsync_remote_command.format(
                node=node,
                source_dir="~/eloqkv-cluster/EloqKV/logs/tx-6389/host_manager.log.INFO",
                destination_dir=os.path.join(
                    node_destination_dir, "host_manager.log.INFO"
                ),
            )
        )
        run_command(
            rsync_remote_command.format(
                node=node,
                source_dir="~/eloqkv-cluster/EloqKV/logs/std-output/std-out-6389",
                destination_dir=os.path.join(node_destination_dir, "std-out-6389"),
            )
        )

    # run_command(rsync_super_server_command)
    # rsync for log_node
    log_node_destination_dir = os.path.join(root_dir, log_node)
    os.makedirs(log_node_destination_dir, exist_ok=True)
    run_command(
        rsync_remote_command.format(
            node=log_node,
            source_dir="/home/eloq/eloqkv-cluster/LogServer/logs/g0n0/log-service.log.INFO",
            destination_dir=os.path.join(
                log_node_destination_dir, "log-service.log.INFO"
            ),
        )
    )

    # remote_root_dir = f"~/{root_dir}"
    # send_email(
    #     f"Detect failures related to Jepsen test or Eloqkv crash. Please refer eloq@compute-1:{remote_root_dir} for more details."
    # )


def flushdb():
    try:
        run_command(flushdb_command, timeout=60)
    except subprocess.TimeoutExpired as e:
        logger.warning(f"Flushdb command timed out: {e}")
        save_error_log()
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        logger.warning(f"Error occurred: {e.stderr}")
        return 1


def check_client_num():
    try:
        for node in node_list:
            result, _ = run_command(redis_command.format(node=node, command="info"))

            # Filter output for "connected_clients"
            for line in result.splitlines():
                if "connected_clients" in line:
                    logger.info(line)  # Print or process the matching line
                    client_num = int(line.split(":")[1])

                    if client_num > 1:
                        logger.warning(
                            "There are unclosed transactions in EloqKV. Please check if any other redis client is connecting."
                        )
                    break

    except subprocess.CalledProcessError as e:
        logger.warning(f"Error occurred: {e.stderr}")


def check_stdout_log():
    # TODO(ZX) need to check log in a period of time
    root_dir = "/home/eloq/workspace/jepsen-eloqkv/.vscode/log/tmp_stdout"
    os.makedirs(root_dir, exist_ok=True)

    for node in node_list:
        node_dir = os.path.join(root_dir, node)
        os.makedirs(node_dir, exist_ok=True)
        stdout_file = os.path.join(node_dir, "std-out-6389")
        run_command(
            rsync_remote_command.format(
                node=node,
                source_dir="~/eloqkv-cluster/EloqKV/logs/std-output/std-out-6389",
                destination_dir=stdout_file,
            )
        )
        if not check_error_in_eloqkv_log(stdout_file):
            logger.warning(f"zx find bug in eloqkv log")
            return False
    return True


def check_error_in_jepsen_log(log_file):
    # Open the log file
    try:
        with open(log_file, "r") as f:
            lines = f.readlines()[-50:]
    except FileNotFoundError:
        logger.warning(f"Log file {log_file} not found.")
        return False
    except Exception as e:
        logger.warning(f"Error reading log file: {e}")
        return False

    # Define the list of error keywords to search for
    error_keywords = ["java.lang.InterruptedException: sleep interrupted"]

    # Check the log content for any of the defined error keywords
    for line in lines:
        for keyword in error_keywords:
            if keyword.lower() in line.lower():  # Case-insensitive search
                logger.error(f"Found error in jepsen log: {line.strip()}")
                return False  # Return after finding the first matching error

    logger.info("No errors found in the last 50 lines of the log.")
    return True


def check_error_in_eloqkv_log(log_file):
    # Open the log file
    try:
        with open(log_file, "r") as f:
            lines = f.readlines()[-50:]
    except FileNotFoundError:
        logger.warning(f"Log file {log_file} not found.")
        return False
    except Exception as e:
        logger.warning(f"Error reading log file: {e}")
        return False

    # Define the list of error keywords to search for
    error_keywords = [
        # "Assertion",
        "assert",
        "asan",
        "AddressSanitizer",
        "Shadow byte",
        "core dumped",
        "Segmentation fault",
    ]

    # Check the log content for any of the defined error keywords
    for line in lines:
        for keyword in error_keywords:
            if keyword.lower() in line.lower():  # Case-insensitive search
                logger.error(f"Found error in eloqkv log: {line.strip()}")
                return False  # Return after finding the first matching error

    logger.info("No errors found in the last 50 lines of the log.")
    return True


def send_email(content):
    logger.info("Send email start.")
    smtp_server = "smtp.qq.com"
    smtp_port = 465
    sender_email = os.getenv("SENDER_EMAIL")
    receiver_email = os.getenv("RECEIVER_EMAIL").split(",")
    password = os.getenv("PASSWORD")
    logger.info(f"sender: {sender_email}")

    # create email
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(receiver_email)
    msg["Subject"] = "Jepsen test fail!"

    # content
    # body = "This is a test email sent from Python using QQ Mail SMTP server."
    msg.attach(MIMEText(content, "plain"))

    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            logger.info("Email sent successfully.")
    except Exception as e:
        logger.warning(f"Error: {e}")
