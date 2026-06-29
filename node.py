import threading
import time
import argparse
import uuid
import socket

from lcr import LCRNode
from heartbeat import HeartbeatMonitor
from server import ChatServer
from discovery import ServerDiscovery


def get_local_ip():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        local_ip = sock.getsockname()[0]
        sock.close()
        return local_ip
    except OSError:
        return "127.0.0.1"


class Node:
    def __init__(self, uid, ring_port, hb_port, chat_port, ip=None):
        self.my_uid = uid
        self.my_ip = ip or get_local_ip()

        self.ring_port = ring_port
        self.hb_port = hb_port
        self.chat_port = chat_port

        self.members = [
            {
                "uid": self.my_uid,
                "ip": self.my_ip,
                "ring_port": self.ring_port,
                "hb_port": self.hb_port,
            }
        ]

        self._leader_services_started = False

        self._lcr = LCRNode(
            my_uid=self.my_uid,
            my_ip=self.my_ip,
            my_ring_port=self.ring_port,
            members=self.members,
            on_leader_elected=self._on_leader_elected,
        )

        self._heartbeat = HeartbeatMonitor(
            my_ip=self.my_ip,
            my_hb_port=self.hb_port,
            on_timeout=self._on_leader_timeout,
        )

        self._chat_server = ChatServer(host=self.my_ip, port=self.chat_port)

        self._discovery = ServerDiscovery(
            my_uid=self.my_uid,
            my_ip=self.my_ip,
            ring_port=self.ring_port,
            hb_port=self.hb_port,
            chat_port=self.chat_port,
            on_members_changed=self._on_members_changed,
            is_leader_func=lambda: self._lcr.is_leader,
        )

    def start(self):
        print("\n" + "=" * 55)
        print(" Node startet")
        print(f" UID       : {self.my_uid}")
        print(f" IP        : {self.my_ip}")
        print(f" Ring-Port : {self.ring_port}")
        print(f" HB-Port   : {self.hb_port}")
        print(f" Chat-Port : {self.chat_port}")
        print("=" * 55 + "\n")

        self._discovery.start()
        self._lcr.start()

        threading.Thread(target=self._election_loop, daemon=True).start()

    def _election_loop(self):
        time.sleep(5)

        while True:
            if not self._lcr.leader_uid and len(self.members) >= 1:
                print("[NODE] Starte Leader Election …")
                self._lcr.initiate_election()

            time.sleep(6)

    def _on_members_changed(self, members):
        self.members = members

        print("[NODE] Aktuelle dynamische Members:")
        for member in self.members:
            print(f"  - {member['uid'][:8]}... Ring-Port={member['ring_port']}")

        self._lcr.update_members(self.members)
        self._heartbeat.update_members(self.members)

        self._lcr.leader_uid = ""
        self._lcr.is_leader = False

        print("[NODE] Member-Änderung erkannt. Neue Election wird gestartet.")

    def _on_leader_elected(self, leader_uid):
        am_leader = leader_uid == self.my_uid

        print("\n[NODE] ✓ Wahl abgeschlossen!")
        print(f"[NODE]   Leader: {leader_uid[:8]}…")
        print(f"[NODE]   Bin ich Leader: {am_leader}\n")

        if am_leader:
            if self._leader_services_started:
                print("[NODE] Leader services already running. Skip restart.")
                self._heartbeat.start(is_leader=True, members=self.members)
                return

            self._leader_services_started = True

            threading.Thread(
                target=self._chat_server.start,
                daemon=True,
                name="chat-server",
            ).start()

            print(f"[NODE] ✓ ChatServer läuft auf Port {self.chat_port}")
            self._heartbeat.start(is_leader=True, members=self.members)

        else:
            if self._leader_services_started:
                print("[NODE] Node is no longer leader. Stopping chat server.")
                self._chat_server.stop()

            self._leader_services_started = False
            self._heartbeat.start(is_leader=False, members=self.members)

    def _on_leader_timeout(self):
        print("\n[NODE] Leader ausgefallen! Starte neue Wahl …\n")

        self._lcr.leader_uid = ""
        self._lcr.is_leader = False
        self._leader_services_started = False

        threading.Thread(target=self._election_loop, daemon=True).start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--uid", type=str, default=None)
    parser.add_argument("--ring-port", type=int, required=True)
    parser.add_argument("--hb-port", type=int, required=True)
    parser.add_argument("--chat-port", type=int, required=True)
    parser.add_argument("--ip", type=str, default=None)

    args = parser.parse_args()

    uid = args.uid or str(uuid.uuid4())

    node = Node(
        uid=uid,
        ring_port=args.ring_port,
        hb_port=args.hb_port,
        chat_port=args.chat_port,
        ip=args.ip,
    )

    node.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[NODE] Knoten wird beendet.")