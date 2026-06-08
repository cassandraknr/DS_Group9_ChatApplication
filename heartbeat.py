"""
Heartbeat Monitor
=================
Detects leader failure by watching for periodic UDP heartbeat datagrams.

The leader sends a heartbeat to each peer's IP every HEARTBEAT_INTERVAL
seconds.  A follower that receives no heartbeat within HEARTBEAT_TIMEOUT
seconds calls on_timeout() so the node layer can trigger a new election.

No third-party libraries are used here.
"""

import socket
import threading
import time


HEARTBEAT_PORT     = 10_002   # UDP port – separate from the ring election port
HEARTBEAT_INTERVAL = 2.0      # seconds between heartbeat sends (leader)
HEARTBEAT_TIMEOUT  = 6.0      # seconds of silence before timeout fires (follower)
HEARTBEAT_MSG      = b"HEARTBEAT"


class HeartbeatMonitor:
    """
    Parameters
    ----------
    my_ip : str
        This node's IP – used to bind the receive socket and to exclude
        self from the peer list when sending.
    on_timeout : callable() | None
        Called (from a background thread) when the leader's heartbeats stop.

    Integration contract
    --------------------
    * After every election, call start(is_leader, members).
    * members is a list of {"uid": str, "ip": str} – the same format used
      by LCRNode, so the two components share one data structure.
    * Call stop() on shutdown or before calling start() again.
    * Call update_members() whenever the group view changes.
    """

    def __init__(self, my_ip: str, on_timeout=None):
        self.my_ip       = my_ip
        self._on_timeout = on_timeout

        self._running    = False
        self._is_leader  = False
        self._members:   list[dict] = []
        self._last_beat: float = time.monotonic()
        self._lock       = threading.Lock()
        self._sock:      socket.socket | None = None

    # ── public API ────────────────────────────────────────────────────────────

    def start(self, is_leader: bool, members: list[dict]):
        """
        (Re-)start the monitor in the correct role after an election.

        Parameters
        ----------
        is_leader : bool
            True  → run the beat loop (send heartbeats to peers)
            False → run the watch loop (listen for the leader's heartbeat)
        members : list of {"uid": str, "ip": str}
            All current group members including this node.
        """
        self.stop()   # tear down any previous loops

        self._is_leader = is_leader
        with self._lock:
            self._members = list(members)
        self._last_beat = time.monotonic()
        self._running   = True

        # Bind receive socket (used for watching; leader also binds so the
        # port stays consistent and the leader can receive its own future
        # role switch without a port conflict).
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.my_ip, HEARTBEAT_PORT))
        self._sock.settimeout(1.0)   # allows the watch loop to check _running

        if is_leader:
            t = threading.Thread(target=self._beat_loop,  daemon=True, name="hb-beat")
            print(f"[HB] I am leader – heartbeats every {HEARTBEAT_INTERVAL}s")
        else:
            t = threading.Thread(target=self._watch_loop, daemon=True, name="hb-watch")
            print(f"[HB] Watching for leader heartbeat (timeout={HEARTBEAT_TIMEOUT}s)")
        t.start()

    def stop(self):
        """Stop all heartbeat activity and close the socket."""
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def update_members(self, members: list[dict]):
        """
        Update the peer list while running.
        Called by the membership layer on group-view changes.

        Parameters
        ----------
        members : list of {"uid": str, "ip": str}
        """
        with self._lock:
            self._members = list(members)

    # ── leader side ───────────────────────────────────────────────────────────

    def _beat_loop(self):
        """Send HEARTBEAT to every peer (all members except self) periodically."""
        send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            while self._running and self._is_leader:
                with self._lock:
                    # Use the IP from each member dict; skip self
                    peers = [m["ip"] for m in self._members if m["ip"] != self.my_ip]
                for peer_ip in peers:
                    try:
                        send_sock.sendto(HEARTBEAT_MSG, (peer_ip, HEARTBEAT_PORT))
                    except OSError as e:
                        print(f"[HB] Could not reach {peer_ip}: {e}")
                time.sleep(HEARTBEAT_INTERVAL)
        finally:
            send_sock.close()

    # ── follower side ─────────────────────────────────────────────────────────

    def _watch_loop(self):
        """Receive heartbeats; trigger an election if they stop arriving."""
        while self._running and not self._is_leader:
            elapsed = time.monotonic() - self._last_beat
            if elapsed > HEARTBEAT_TIMEOUT:
                print(f"[HB] ✗ No heartbeat for {elapsed:.1f}s – triggering election …")
                self._running = False
                if self._on_timeout:
                    self._on_timeout()
                return

            try:
                data, _ = self._sock.recvfrom(256)
                if data == HEARTBEAT_MSG:
                    self._last_beat = time.monotonic()
            except socket.timeout:
                pass    # normal – loop back and re-check elapsed time
            except OSError:
                break
