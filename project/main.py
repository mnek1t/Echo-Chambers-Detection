import subprocess
import time
import signal
import sys
from dotenv import load_dotenv
load_dotenv()
processes = []

def start_process(cmd, name):
    print(f"[START] {name}")
    p = subprocess.Popen(cmd)
    processes.append(p)

def shutdown(signum, frame):
    print("\n[SHUTDOWN] Stopping all processes...")
    for p in processes:
        p.terminate()
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    start_process(
        ["python3", "neo4j/consumer_user.py"],
        "Neo4j User Consumer"
    )

    start_process(
        ["python3", "neo4j/consumer_post.py"],
        "Neo4j Post Consumer"
    )

    start_process(
        ["python3", "qdrant/consumer_post.py"],
        "Qdrant Post Consumer"
    )

    start_process(
        ["python3", "postgres/community.py"],
        "Postgres"
    )

    time.sleep(3)

    start_process(
        ["python3", "kafka/producer_main.py"],
        "Kafka Producer"
    )

    print("\n[RUNNING] All processes started. Press Ctrl+C to stop.\n")

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
