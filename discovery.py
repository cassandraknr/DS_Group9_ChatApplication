import socket
import threading
import json
import time
from typing import Callable

DISCOVERY_PORT = 5007
BROADCAST_IP = "255.255.255.255"
BUFFER_SIZE = 4096


def create_udp_socket(bind_port=None, broadcast=False):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    if broadcast:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    if bind_port is not None:
        sock.bind(("", bind_port))

    sock.settimeout(1.0)
    return sock


class ServerDiscovery:
    def __init__(
        self,
        my_uid,
        my_ip,
        ring_port,
        hb_port,
        chat_port,
        on_members_changed=None,
        is_leader_func=None,
    ):
        self.my_uid = my_uid
        self.my_ip = my_ip
        self.ring_port = ring_port
        self.hb_port = hb_port
        self.chat_port = chat_port

        self.on_members_changed = on_members_changed
        self.is_leader_func = is_leader_func

        self.running = False
        self.members = {
            self.my_uid: {
                "uid": self.my_uid,
                "ip": self.my_ip,
                "ring_port": self.ring_port,
                "hb_port": self.hb_port,
                "chat_port": self.chat_port,
                "last_seen": time.time(),
            }
        }

    def start(self):
        self.running = True
        threading.Thread(target=self._announce_loop, daemon=True).start()
        threading.Thread(target=self._listen_loop, daemon=True).start()
        threading.Thread(target=self._cleanup_loop, daemon=True).start()
        print("[DISCOVERY] Server discovery started.")

    def stop(self):
        self.running = False

    def get_members(self):
        return [
            {
                "uid": m["uid"],
                "ip": m["ip"],
                "ring_port": m["ring_port"],
                "hb_port": m["hb_port"],
            }
            for m in self.members.values()
        ]

    def _announce_loop(self):
        sock = create_udp_socket(broadcast=True)

        while self.running:
            msg = {
                "type": "SERVER_ANNOUNCE",
                "uid": self.my_uid,
                "ip": self.my_ip,
                "ring_port": self.ring_port,
                "hb_port": self.hb_port,
                "chat_port": self.chat_port,
            }

            sock.sendto(json.dumps(msg).encode("utf-8"), (BROADCAST_IP, DISCOVERY_PORT))
            time.sleep(2)

    def _listen_loop(self):
        sock = create_udp_socket(bind_port=DISCOVERY_PORT, broadcast=True)

        while self.running:
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode("utf-8"))

                if msg.get("type") == "SERVER_ANNOUNCE":
                    self._handle_server_announce(msg)

                elif msg.get("type") == "DISCOVER_LEADER":
                    self._handle_leader_discovery(addr)

            except socket.timeout:
                pass
            except OSError:
                break
            except Exception as e:
                print(f"[DISCOVERY] Error: {e}")

    def _handle_server_announce(self, msg):
        uid = msg["uid"]

        old_members = set(self.members.keys())

        self.members[uid] = {
            "uid": uid,
            "ip": msg["ip"],
            "ring_port": msg["ring_port"],
            "hb_port": msg["hb_port"],
            "chat_port": msg["chat_port"],
            "last_seen": time.time(),
        }

        new_members = set(self.members.keys())

        if old_members != new_members:
            print(f"[DISCOVERY] New server discovered: {uid[:8]}...")
            if self.on_members_changed:
                self.on_members_changed(self.get_members())

    def _handle_leader_discovery(self, addr):
        if self.is_leader_func and self.is_leader_func():
            response = {
                "type": "LEADER_RESPONSE",
                "leader_ip": self.my_ip,
                "chat_port": self.chat_port,
            }

            sock = create_udp_socket()
            sock.sendto(json.dumps(response).encode("utf-8"), addr)
            sock.close()

    def _cleanup_loop(self):
        while self.running:
            now = time.time()
            removed = []

            for uid, member in list(self.members.items()):
                if uid == self.my_uid:
                    continue

                if now - member["last_seen"] > 8:
                    removed.append(uid)
                    del self.members[uid]

            if removed:
                print(f"[DISCOVERY] Removed inactive servers: {[uid[:8] for uid in removed]}")
                if self.on_members_changed:
                    self.on_members_changed(self.get_members())

            time.sleep(3)


class DiscoveryClient:
    def __init__(self, timeout=3.0, retries=5):
        self.timeout = timeout
        self.retries = retries

    def find_leader(self):
        sock = create_udp_socket(broadcast=True)
        sock.bind(("", 0))
        sock.settimeout(self.timeout)

        msg = {"type": "DISCOVER_LEADER"}

        for attempt in range(1, self.retries + 1):
            print(f"[DISCOVERY] Suche Leader … (Versuch {attempt}/{self.retries})")

            sock.sendto(json.dumps(msg).encode("utf-8"), (BROADCAST_IP, DISCOVERY_PORT))

            try:
                data, _ = sock.recvfrom(BUFFER_SIZE)
                response = json.loads(data.decode("utf-8"))

                if response.get("type") == "LEADER_RESPONSE":
                    leader_ip = response["leader_ip"]
                    chat_port = response["chat_port"]

                    print(f"[DISCOVERY] ✓ Leader gefunden: {leader_ip}:{chat_port}")
                    sock.close()
                    return leader_ip, chat_port

            except socket.timeout:
                print("[DISCOVERY] Keine Antwort.")
                time.sleep(1)

        sock.close()
        return None