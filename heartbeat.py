"""
Heartbeat Monitor – Windows-kompatible Version
Jeder Knoten hat seinen eigenen Heartbeat-Port.
"""

import socket
import threading
import time

HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT  = 6.0
HEARTBEAT_MSG      = b"HEARTBEAT"


class HeartbeatMonitor:

    def __init__(self, my_ip, my_hb_port, on_timeout=None):
        self.my_ip       = my_ip
        self.my_hb_port  = my_hb_port   # eigener Heartbeat-Port
        self._on_timeout = on_timeout

        self._running    = False
        self._is_leader  = False
        self._members    = []
        self._last_beat  = time.monotonic()
        self._lock       = threading.Lock()
        self._sock       = None

    def start(self, is_leader, members):
        self.stop()
        self._is_leader = is_leader
        with self._lock:
            self._members = list(members)
        self._last_beat = time.monotonic()
        self._running   = True

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("0.0.0.0", self.my_hb_port))
        self._sock.settimeout(1.0)

        if is_leader:
            t = threading.Thread(target=self._beat_loop,  daemon=True, name="hb-beat")
            print(f"[HB] Ich bin Leader – sende Heartbeat alle {HEARTBEAT_INTERVAL}s")
        else:
            t = threading.Thread(target=self._watch_loop, daemon=True, name="hb-watch")
            print(f"[HB] Überwache Leader-Heartbeat (Timeout={HEARTBEAT_TIMEOUT}s)")
        t.start()

    def stop(self):
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def update_members(self, members):
        with self._lock:
            self._members = list(members)

    def _beat_loop(self):
        send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            while self._running and self._is_leader:
                with self._lock:
                    # Jeden Peer auf seinem eigenen hb_port erreichen
                    peers = [(m["ip"], m["hb_port"]) for m in self._members if m["ip"] != self.my_ip or m["hb_port"] != self.my_hb_port]
                for peer_ip, peer_hb_port in peers:
                    try:
                        send_sock.sendto(HEARTBEAT_MSG, (peer_ip, peer_hb_port))
                    except OSError as e:
                        print(f"[HB] Konnte {peer_ip}:{peer_hb_port} nicht erreichen: {e}")
                time.sleep(HEARTBEAT_INTERVAL)
        finally:
            send_sock.close()

    def _watch_loop(self):
        while self._running and not self._is_leader:
            elapsed = time.monotonic() - self._last_beat
            if elapsed > HEARTBEAT_TIMEOUT:
                print(f"[HB] ✗ Kein Heartbeat seit {elapsed:.1f}s – starte neue Wahl…")
                self._running = False
                if self._on_timeout:
                    self._on_timeout()
                return
            try:
                data, _ = self._sock.recvfrom(256)
                if data == HEARTBEAT_MSG:
                    self._last_beat = time.monotonic()
            except socket.timeout:
                pass
            except OSError:
                break
