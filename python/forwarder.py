import socket
import json
import certstream
import os
import time
from dotenv import load_dotenv

load_dotenv()
NIFI_SERVER_IP = os.getenv("NIFI_SERVER_IP")
NIFI_SERVER_PORT = int(os.getenv("NIFI_SERVER_PORT"))

def send_to_nifi(sock, message):
    try:
        data = json.dumps(message) + "\n"
        sock.sendall(data.encode('utf-8'))
    except Exception as e:
        print(f"[-] Error sending data to NiFi: {e}")
        raise

def callback(message, context):
    global nifi_socket
    if message.get("message_type") == "dns_entries":
        try:
            send_to_nifi(nifi_socket, message)
        except:
            print(f"[-] Error sending to NiFi ({e}), reconnecting...")
            nifi_socket = reconnect_nifi()

def reconnect_nifi():
    while True:
        try:
            print(f"[*] Connecting to NiFi at {NIFI_SERVER_IP}:{NIFI_SERVER_PORT}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((NIFI_SERVER_IP, NIFI_SERVER_PORT))
            print("[+] Connected to NiFi!")
            return s
        except Exception:
            print(f"[-] NiFi connection failed, ({e}), retrying in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    nifi_socket = reconnect_nifi()
    print("[*] Listening to local CertStream Rust server...")
    certstream.listen_for_events(callback, url='ws://localhost:8080/domains-only')
