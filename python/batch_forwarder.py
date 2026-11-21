import json
import os
import queue
import socket
import threading
import time
import certstream
from dotenv import load_dotenv

load_dotenv()

NIFI_SERVER_IP = os.getenv("NIFI_SERVER_IP")
port_str = os.getenv("NIFI_SERVER_PORT")
NIFI_SERVER_PORT = int(port_str) if port_str else 9999

# Larger batches = fewer FlowFiles, no dropped events
BATCH_SIZE = 500
FLUSH_INTERVAL = 1.0

if not NIFI_SERVER_IP:
    raise ValueError("[-] NIFI_SERVER_IP is not set in your .env file!")

event_queue = queue.Queue()
socket_lock = threading.Lock()
nifi_socket = None


def connect_nifi():
    while True:
        try:
            print(f"[*] Connecting to NiFi at {NIFI_SERVER_IP}:{NIFI_SERVER_PORT}...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((NIFI_SERVER_IP, NIFI_SERVER_PORT))
            print("[+] Connected to NiFi!")
            return sock
        except Exception as e:
            print(f"[-] NiFi connection failed ({e}), retrying in 5s...")
            time.sleep(5)


def flush_batch(batch):
    global nifi_socket
    if not batch:
        return

    payload = "".join(batch).encode("utf-8")

    with socket_lock:
        while True:
            try:
                if nifi_socket is None:
                    nifi_socket = connect_nifi()
                nifi_socket.sendall(payload)
                return
            except Exception as e:
                print(f"[-] Error sending batch to NiFi ({e}), reconnecting...")
                try:
                    if nifi_socket is not None:
                        nifi_socket.close()
                except Exception:
                    pass
                nifi_socket = None
                time.sleep(1)


def batch_worker():
    batch = []
    last_flush = time.monotonic()

    while True:
        timeout = max(0.05, FLUSH_INTERVAL - (time.monotonic() - last_flush))
        try:
            item = event_queue.get(timeout=timeout)
            batch.append(item)
        except queue.Empty:
            pass

        now = time.monotonic()
        if len(batch) >= BATCH_SIZE or (batch and (now - last_flush) >= FLUSH_INTERVAL):
            flush_batch(batch)
            batch.clear()
            last_flush = now


def callback(message, context):
    if message.get("message_type") in ("dns_entries", "certificate_update"):
        event_queue.put(json.dumps(message) + "\n")


if __name__ == "__main__":
    nifi_socket = connect_nifi()

    worker = threading.Thread(target=batch_worker, daemon=True)
    worker.start()

    print("[*] Starting high-volume batched CertStream forwarder...")
    certstream.listen_for_events(callback, url="ws://localhost:8080/domains-only")
