import socket
import threading
import json
import uuid


def generate_uid() -> str:
    return str(uuid.uuid4())


def form_ring(members: list[dict]) -> list[dict]:
    return sorted(members, key=lambda m: m["uid"])


def get_neighbour(ring: list[dict], my_uid: str) -> dict | None:
    uids = [m["uid"] for m in ring]
    if my_uid not in uids:
        return None
    idx = uids.index(my_uid)
    return ring[(idx + 1) % len(ring)]


BUFFER_SIZE = 4096


class LCRNode:

    def __init__(self, my_uid, my_ip, my_ring_port, members, on_leader_elected=None):
        self.my_uid       = my_uid
        self.my_ip        = my_ip
        self.my_ring_port = my_ring_port

        self.participant  = False
        self.leader_uid   = ""
        self.is_leader    = False

        self._on_leader_elected = on_leader_elected
        self._lock      = threading.Lock()
        self._recv_sock = None
        self._send_sock = None
        self._running   = False

        self._members   = members
        self._ring      = form_ring(members)
        self._neighbour = get_neighbour(self._ring, self.my_uid)

    def start(self):
        self._recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._recv_sock.bind(("0.0.0.0", self.my_ring_port))
        self._recv_sock.settimeout(1.0)

        self._send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self._running = True
        threading.Thread(target=self._receive_loop, daemon=True, name="lcr-recv").start()

        if self._neighbour:
            print(f"[LCR] Port {self.my_ring_port} | Nachbar: Port {self._neighbour['ring_port']}")
        else:
            print(f"[LCR] Port {self.my_ring_port} | Kein Nachbar!")

    def stop(self):
        self._running = False
        for s in [self._recv_sock, self._send_sock]:
            if s:
                try: s.close()
                except OSError: pass

    def update_members(self, members):
        with self._lock:
            self._members   = members
            self._ring      = form_ring(members)
            self._neighbour = get_neighbour(self._ring, self.my_uid)

    def reset_leader(self):
        """Threadsicherer Reset des Leader-Zustands (statt direktem Zugriff von außen)."""
        with self._lock:
            self.leader_uid  = ""
            self.is_leader   = False
            self.participant = False

    def initiate_election(self):
        with self._lock:
            self.participant = True
            self.is_leader   = False
        print(f"[LCR] Starte Wahl → sende an Port {self._neighbour['ring_port'] if self._neighbour else '???'}")
        self._send_msg({"mid": self.my_uid, "isLeader": False})

    def _receive_loop(self):
        while self._running:
            try:
                data, addr = self._recv_sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode("utf-8"))
                print(f"[LCR] ← Empfangen: mid={msg['mid'][:8]}… isLeader={msg['isLeader']}")
                self._process(msg)
            except socket.timeout:
                pass
            except OSError:
                break
            except Exception as e:
                print(f"[LCR] Fehler: {e}")

    def _process(self, msg):
        m             = msg["mid"]
        is_leader_msg = msg["isLeader"]

        to_send = None   # Nachricht die weitergeleitet werden soll
        callback = None  # Callback der aufgerufen werden soll

        # Lock nur für Zustandsänderung
        with self._lock:
            if is_leader_msg:
                already_known = (self.leader_uid == m and self.leader_uid != "")
                self.leader_uid  = m
                self.is_leader   = (m == self.my_uid)
                self.participant = False

                if already_known:
                    # Diese Coordinator-Nachricht wurde bereits verarbeitet
                    # (Duplikat/Ring-Inkonsistenz) -> hier stoppen, nicht weiterleiten.
                    print(f"[LCR] Duplikat-Coordinator ignoriert: mid={m[:8]}…")
                    return
                callback = self._on_leader_elected
                if not self.is_leader:
                    to_send = msg  # weiterleiten (außer wenn zurück bei mir)

            elif m < self.my_uid:
                if not self.participant:
                    self.participant = True
                    to_send = {"mid": self.my_uid, "isLeader": False}
                    print(f"[LCR] Kleinere UID → sende eigene UID weiter")
                else:
                    print(f"[LCR] Kleinere UID → verwerfe")

            elif m > self.my_uid:
                self.participant = True
                to_send = msg
                print(f"[LCR] Größere UID → leite weiter")

            else:  # m == my_uid → ich habe gewonnen!
                self.leader_uid  = self.my_uid
                self.is_leader   = True
                self.participant = False
                to_send  = {"mid": self.my_uid, "isLeader": True}
                callback = self._on_leader_elected
                print(f"[LCR] ✓ Ich habe gewonnen! Sende COORDINATOR…")

        # Senden und Callback außerhalb des Locks
        if to_send:
            self._send_msg(to_send)
        if callback:
            if self.is_leader:
                print(f"[LCR] ✓ Ich bin bestätigter Leader!")
            else:
                print(f"[LCR] Neuer Leader: {self.leader_uid[:8]}…")
            callback(self.leader_uid)

    def _send_msg(self, msg):
        neighbour = self._neighbour
        if neighbour is None:
            print("[LCR] Kein Nachbar!")
            return
        try:
            data = json.dumps(msg).encode("utf-8")
            print(f"[LCR] Sende an {neighbour['ip']}:{neighbour['ring_port']}")
            self._send_sock.sendto(data, (neighbour["ip"], neighbour["ring_port"]))
            print(f"[LCR] → Gesendet an Port {neighbour['ring_port']}: mid={msg['mid'][:8]}… isLeader={msg['isLeader']}")
        except OSError as e:
            print(f"[LCR] Sendefehler: {e}")
