"""
Cluster Health & Port Verification Script
Checks reachability of Hadoop Ecosystem components:
- HDFS NameNode (9870 / 9000)
- YARN ResourceManager (8088)
- Hive Server2 (10000 / 10002)
- HBase Master & Web UI (16010 / 8080)
- ZooKeeper (2181)
"""

import socket
import sys

SERVICES = [
    ("HDFS NameNode Web UI", "localhost", 9870),
    ("HDFS NameNode IPC", "localhost", 9000),
    ("YARN ResourceManager UI", "localhost", 8088),
    ("Apache Hive Thrift/Server", "localhost", 10000),
    ("Apache HBase Master UI", "localhost", 16010),
    ("Apache HBase REST API", "localhost", 8080),
    ("Apache ZooKeeper Quorum", "localhost", 2181),
]

def check_port(name: str, host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            print(f"  [ONLINE]  {name:30} -> {host}:{port}")
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        print(f"  [OFFLINE] {name:30} -> {host}:{port}")
        return False

def main():
    print("=" * 65)
    print("Hadoop Ecosystem Cluster Health Verification")
    print("=" * 65)
    
    online_count = 0
    for name, host, port in SERVICES:
        if check_port(name, host, port):
            online_count += 1
            
    print("-" * 65)
    print(f"Status Summary: {online_count} / {len(SERVICES)} services active.")
    if online_count == 0:
        print("\nNote: Docker cluster is currently stopped or running in Standalone/Hybrid mode.")
        print("To start the full Docker container stack: docker compose up -d")
        print("To run in zero-overhead Hybrid mode: use the local ingestion & analytics pipeline directly.")
    print("=" * 65)

if __name__ == "__main__":
    main()
