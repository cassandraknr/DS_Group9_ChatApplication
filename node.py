"""
node.py – Windows-kompatible Version mit automatischer Wahlwiederholung
========================================================================

Alle 3 Knoten laufen auf 127.0.0.1 mit verschiedenen Ports.
Feste UIDs damit alle Knoten dieselbe Ring-Reihenfolge kennen.
Knoten 3 gewinnt immer (größte UUID).

Verwendung:
  python node.py --id 1
  python node.py --id 2
  python node.py --id 3
"""

import threading
import time
import argparse

from lcr       import LCRNode
from heartbeat import HeartbeatMonitor
from server    import ChatServer
from discovery import DiscoveryListener

IP = "127.0.0.1"

# ══════════════════════════════════════════════════════════════════
# FESTE KONFIGURATION – alle Knoten kennen diese Tabelle
# Feste UIDs → alle Knoten einig über Ring-Reihenfolge
# Knoten 3 hat größte UUID → gewinnt immer die Wahl
# ══════════════════════════════════════════════════════════════════
NODES = {
    1: {
        "uid":       "1111aaaa-0001-0001-0001-000000000001",
        "ring_port": 10001,
        "hb_port":   10101,
        "chat_port": 5001,
    },
    2: {
        "uid":       "2222bbbb-0002-0002-0002-000000000002",
        "ring_port": 10002,
        "hb_port":   10102,
        "chat_port": 5002,
    },
    3: {
        "uid":       "3333cccc-0003-0003-0003-000000000003",
        "ring_port": 10003,
        "hb_port":   10103,
        "chat_port": 5003,
    },
}

# Mitgliederliste im Format das LCR und Heartbeat brauchen
MEMBERS = [
    {"uid": NODES[i]["uid"], "ip": IP,
     "ring_port": NODES[i]["ring_port"],
     "hb_port":   NODES[i]["hb_port"]}
    for i in NODES
]


class Node:

    def __init__(self, node_id: int):
        self.node_id  = node_id
        cfg = NODES[node_id]

        self.my_uid   = cfg["uid"]
        self.my_ip    = IP
        self.ring_port = cfg["ring_port"]
        self.hb_port   = cfg["hb_port"]
        self.chat_port = cfg["chat_port"]

        self._lcr = LCRNode(
            my_uid           = self.my_uid,
            my_ip            = self.my_ip,
            my_ring_port     = self.ring_port,
            members          = MEMBERS,
            on_leader_elected= self._on_leader_elected,
        )

        self._heartbeat = HeartbeatMonitor(
            my_ip      = self.my_ip,
            my_hb_port = self.hb_port,
            on_timeout = self._on_leader_timeout,
        )

        self._chat_server = ChatServer(host=self.my_ip, port=self.chat_port)
        self._discovery   = DiscoveryListener(my_ip=self.my_ip, chat_port=self.chat_port)

    def start(self):
        print(f"\n{'='*55}")
        print(f"  Knoten {self.node_id} startet")
        print(f"  UUID      : {self.my_uid}")
        print(f"  Ring-Port : {self.ring_port}")
        print(f"  HB-Port   : {self.hb_port}")
        print(f"  Chat-Port : {self.chat_port}")
        print(f"{'='*55}\n")

        # LCR-Ring-Socket starten
        self._lcr.start()

        # Wahl-Loop starten – wiederholt alle 5s bis ein Leader gefunden ist
        threading.Thread(target=self._election_loop, daemon=True, name="election-loop").start()

    def _election_loop(self):
        """
        Startet alle 5 Sekunden eine neue Wahl, bis ein Leader bekannt ist.
        So spielt es keine Rolle in welcher Reihenfolge die Knoten starten.
        """
        time.sleep(2)  # kurz warten bis Socket bereit ist
        while not self._lcr.leader_uid:
            print(f"[NODE] Starte Wahl (kein Leader bekannt)…")
            self._lcr.initiate_election()
            time.sleep(5)  # 5 Sekunden warten, dann nochmal falls nötig

    def _on_leader_elected(self, leader_uid: str):
        am_leader = (leader_uid == self.my_uid)
        print(f"\n[NODE] ✓ Wahl abgeschlossen!")
        print(f"[NODE]   Leader: {leader_uid[:8]}…")
        print(f"[NODE]   Bin ich Leader: {am_leader}\n")

        if am_leader:
            threading.Thread(target=self._chat_server.start, daemon=True, name="chat-server").start()
            print(f"[NODE] ✓ ChatServer läuft auf Port {self.chat_port}")
            self._discovery.start()
            self._heartbeat.start(is_leader=True, members=MEMBERS)
        else:
            self._heartbeat.start(is_leader=False, members=MEMBERS)

    def _on_leader_timeout(self):
        print("\n[NODE] Leader ausgefallen! Starte neue Wahl…\n")
        self._lcr.leader_uid = ""  # zurücksetzen damit election_loop neu startet
        self._discovery.stop()
        threading.Thread(target=self._election_loop, daemon=True).start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True, choices=[1, 2, 3],
                        help="Knoten-ID: 1, 2 oder 3")
    args = parser.parse_args()

    node = Node(node_id=args.id)
    node.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[NODE] Knoten wird beendet.")
