"""
LCR (LeLann-Chang-Roberts) Leader Election Algorithm
=====================================================
The ring is ordered and identified entirely by UUID.
IP addresses are only used at the transport layer (UDP sendto needs an IP).

No third-party libraries are used for the core algorithm.
The 'uuid' module is only used to *generate* unique identifiers.

Member representation
---------------------
Every node is represented as a dict:
    {"uid": "<uuid-string>", "ip": "<dotted-decimal>"}

The ring is the sorted-by-UUID list of those dicts.
"""

import socket
import threading
import json
import uuid


# ─────────────────────────────────────────────────────────────────────────────
# Ring helpers
# ─────────────────────────────────────────────────────────────────────────────

def generate_uid() -> str:
    """Generate a universally unique identifier using the uuid library."""
    return str(uuid.uuid4())


def form_ring(members: list[dict]) -> list[dict]:
    """
    Return the member list sorted by UUID string.
    Every node runs the same sort, so the ring order is consistent
    without any coordination.

    Parameters
    ----------
    members : list of {"uid": str, "ip": str}
    """
    return sorted(members, key=lambda m: m["uid"])


def get_neighbour(ring: list[dict], my_uid: str, direction: str = "left") -> dict | None:
    """
    Return the left (clockwise) or right (counter-clockwise) neighbour
    of the node with *my_uid* in *ring*, wrapping around at both ends.

    Returns None when this node is not found in the ring.
    """
    uids = [m["uid"] for m in ring]
    if my_uid not in uids:
        return None
    idx = uids.index(my_uid)
    if direction == "left":           # clockwise
        return ring[(idx + 1) % len(ring)]
    else:                             # counter-clockwise
        return ring[(idx - 1) % len(ring)]


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

RING_PORT   = 10_001
BUFFER_SIZE = 4096


# ─────────────────────────────────────────────────────────────────────────────
# LCR Node
# ─────────────────────────────────────────────────────────────────────────────

class LCRNode:
    """
    One participant in the LCR ring-based leader election.

    Parameters
    ----------
    my_uid : str
        This node's UUID (generated with generate_uid()).
    my_ip : str
        This node's IP address – used only to bind the UDP socket.
    members : list of {"uid": str, "ip": str}
        All known group members, including this node.
    on_leader_elected : callable(leader_uid: str) | None
        Called whenever a new leader is determined.
        Receives the winning UUID as its only argument.

    Integration contract
    --------------------
    * Call start()            – bind socket, start receive loop.
    * Call initiate_election() – kick off (or re-start) an election.
    * Read self.leader_uid    – UUID of the current leader ("" if unknown).
    * Read self.is_leader     – True if this node is the current leader.
    """

    def __init__(
        self,
        my_uid: str,
        my_ip: str,
        members: list[dict],
        on_leader_elected=None,
    ):
        self.my_uid: str = my_uid
        self.my_ip:  str = my_ip

        # Election state
        self.participant:  bool = False
        self.leader_uid:   str  = ""
        self.is_leader:    bool = False

        self._on_leader_elected = on_leader_elected
        self._lock   = threading.Lock()
        self._sock:  socket.socket | None = None
        self._running: bool = False

        # Build ring from the initial member list
        self._members: list[dict] = members
        self._ring:      list[dict] = form_ring(members)
        self._neighbour: dict | None = get_neighbour(self._ring, self.my_uid)

    # ── public API ────────────────────────────────────────────────────────────

    def start(self):
        """Bind the ring UDP socket and start the background receive loop."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.my_ip, RING_PORT))
        self._running = True
        t = threading.Thread(target=self._receive_loop, daemon=True, name="lcr-recv")
        t.start()
        print(f"[LCR] Node UID={self.my_uid[:8]}… IP={self.my_ip} listening on :{RING_PORT}")
        print(f"[LCR] Ring order: {[m['uid'][:8] + '…' for m in self._ring]}")
        print(f"[LCR] Clockwise neighbour: {self._neighbour['uid'][:8]}… ({self._neighbour['ip']})"
              if self._neighbour else "[LCR] No neighbour (single-node ring)")

    def stop(self):
        """Shut the node down cleanly."""
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass

    def update_members(self, members: list[dict]):
        """
        Called by the membership layer whenever the group view changes.
        Rebuilds the ring and updates the clockwise neighbour pointer.

        Parameters
        ----------
        members : list of {"uid": str, "ip": str}
        """
        with self._lock:
            self._members  = members
            self._ring     = form_ring(members)
            self._neighbour = get_neighbour(self._ring, self.my_uid)
        print(f"[LCR] Ring updated → {[m['uid'][:8] + '…' for m in self._ring]}")
        print(f"[LCR] New neighbour: {self._neighbour['uid'][:8]}… ({self._neighbour['ip']})"
              if self._neighbour else "[LCR] No neighbour after update")

    def initiate_election(self):
        """
        Start a new election round.
        Call this on startup or when the heartbeat monitor signals leader failure.
        """
        with self._lock:
            self.participant = True
            self.is_leader   = False
        print(f"[LCR] UID={self.my_uid[:8]}… initiating election …")
        self._send_election(self.my_uid, is_leader=False)

    # ── internal: receive loop ────────────────────────────────────────────────

    def _receive_loop(self):
        while self._running:
            try:
                data, _ = self._sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode("utf-8"))
                self._process(msg)
            except OSError:
                break
            except (json.JSONDecodeError, KeyError) as e:
                print(f"[LCR] Malformed message ignored: {e}")

    # ── internal: core LCR algorithm ─────────────────────────────────────────

    def _process(self, msg: dict):
        """
        Core LCR decision logic from the lecture slides.

        Message fields
        --------------
        mid      : str   – UUID of the candidate
        isLeader : bool  – False = election probe, True = leader announcement
        """
        m          = msg["mid"]
        is_leader_msg = msg["isLeader"]

        with self._lock:

            # ── Case 1: leader announcement travelling the ring ───────────────
            if is_leader_msg:
                self.leader_uid  = m
                self.is_leader   = (m == self.my_uid)
                self.participant = False

                if self.is_leader:
                    # Announcement completed the full loop back to the winner;
                    # do NOT forward further.
                    print(f"[LCR] ✓ I (UID={self.my_uid[:8]}…) am confirmed leader.")
                    if self._on_leader_elected:
                        self._on_leader_elected(self.leader_uid)
                    return

                print(f"[LCR] Leader elected: UID={m[:8]}…")
                if self._on_leader_elected:
                    self._on_leader_elected(self.leader_uid)
                # Forward the announcement to the next node
                self._send_raw(msg)

            # ── Case 2: incoming UID smaller than mine ────────────────────────
            elif m < self.my_uid:
                if not self.participant:
                    # Replace with my own UID and become a participant
                    self.participant = True
                    self._send_election(self.my_uid, is_leader=False)
                # Already participant → discard (per LCR spec)

            # ── Case 3: incoming UID larger than mine ─────────────────────────
            elif m > self.my_uid:
                self.participant = True
                self._send_raw(msg)          # forward as-is

            # ── Case 4: message came back to me → I have the highest UID ──────
            else:   # m == self.my_uid
                self.leader_uid  = self.my_uid
                self.is_leader   = True
                self.participant = False
                print(f"[LCR] ✓ I (UID={self.my_uid[:8]}…) won the election! Announcing …")
                self._send_election(self.my_uid, is_leader=True)
                if self._on_leader_elected:
                    self._on_leader_elected(self.leader_uid)

    # ── internal: send helpers ────────────────────────────────────────────────

    def _send_election(self, mid: str, is_leader: bool):
        self._send_raw({"mid": mid, "isLeader": is_leader})

    def _send_raw(self, msg: dict):
        """Send *msg* as JSON to the clockwise neighbour's IP."""
        neighbour = self._neighbour
        if neighbour is None:
            print("[LCR] No neighbour – cannot forward.")
            return
        try:
            data = json.dumps(msg).encode("utf-8")
            self._sock.sendto(data, (neighbour["ip"], RING_PORT))
        except OSError as e:
            print(f"[LCR] Send failed to {neighbour['ip']}: {e}")
