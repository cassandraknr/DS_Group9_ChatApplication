"""
discovery.py – Dynamic Leader Discovery via UDP Multicast
=========================================================

Zwei Klassen:

  DiscoveryListener  → läuft auf dem Leader-Knoten als Hintergrund-Thread
                       Empfängt Multicast-Anfragen und antwortet mit
                       IP + Chat-Port des Leaders

  DiscoveryClient    → läuft im ChatClient
                       Sendet Multicast-Anfrage, wartet auf Antwort des Leaders
                       Gibt (ip, port) zurück

Ersetzt den Placeholder in client.py:
    def discover_leader(self):
        # Placeholder for UDP Discovery  ← wird durch DiscoveryClient ersetzt
"""

import socket
import struct
import json
import threading
import time

# ══════════════════════════════════════════════════════════════════
# KONFIGURATION – muss in allen Files identisch sein
# ══════════════════════════════════════════════════════════════════
MCAST_GROUP = "224.1.1.1"
MCAST_PORT  = 5007


# ══════════════════════════════════════════════════════════════════
# TEIL 1: LISTENER (läuft auf dem Leader-Knoten)
#
# Der Leader lauscht auf Multicast-Anfragen.
# Kommt eine DISCOVER_LEADER-Nachricht an, antwortet er direkt
# per Unicast mit seiner IP + Chat-Port an den anfragenden Client.
# ══════════════════════════════════════════════════════════════════
class DiscoveryListener:
    """
    Wird in node.py aufgerufen, sobald dieser Knoten Leader wird.

    Beispiel:
        listener = DiscoveryListener(my_ip="127.0.0.1", chat_port=5000)
        listener.start()   # wenn ich Leader werde
        listener.stop()    # wenn ich nicht mehr Leader bin
    """

    def __init__(self, my_ip: str, chat_port: int):
        self.my_ip     = my_ip
        self.chat_port = chat_port
        self._running  = False
        self._sock     = None

    def start(self):
        """Startet den Listener als Hintergrund-Thread."""
        self._running = True
        threading.Thread(target=self._listen_loop, daemon=True, name="discovery-listener").start()
        print(f"[DISCOVERY] Listener aktiv – lausche auf {MCAST_GROUP}:{MCAST_PORT}")

    def stop(self):
        """Stoppt den Listener."""
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass

    def _listen_loop(self):
        # UDP-Socket für Multicast-Empfang erstellen
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("", MCAST_PORT))

        # Multicast-Gruppe "abonnieren" – wie Newsletter anmelden
        group = struct.pack("4sL", socket.inet_aton(MCAST_GROUP), socket.INADDR_ANY)
        self._sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, group)
        self._sock.settimeout(1.0)

        while self._running:
            try:
                data, addr = self._sock.recvfrom(1024)
                msg = json.loads(data.decode())

                if msg.get("type") == "DISCOVER_LEADER":
                    # ✅ Client fragt "Wer ist Leader?" → ich antworte
                    print(f"[DISCOVERY] Anfrage von {addr[0]} – ich bin Leader, antworte.")
                    response = json.dumps({
                        "type":      "LEADER_RESPONSE",
                        "leader_ip": self.my_ip,
                        "chat_port": self.chat_port,
                    }).encode()
                    # Direkte Unicast-Antwort zurück an den Client
                    reply = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    reply.sendto(response, addr)
                    reply.close()

            except socket.timeout:
                pass  # Normal – weiter lauschen
            except OSError:
                break


# ══════════════════════════════════════════════════════════════════
# TEIL 2: CLIENT (ersetzt Placeholder in client.py)
#
# Sendet DISCOVER_LEADER per Multicast ins Netz.
# Wartet auf Antwort des Leaders.
# Gibt (ip, port) zurück – fertig.
# ══════════════════════════════════════════════════════════════════
class DiscoveryClient:
    """
    Wird in ChatClient.discover_leader() aufgerufen.

    Beispiel:
        dc = DiscoveryClient()
        result = dc.find_leader()
        if result:
            ip, port = result
    """

    def __init__(self, timeout: float = 3.0, retries: int = 5):
        self.timeout = timeout
        self.retries = retries

    def find_leader(self) -> tuple[str, int] | None:
        """
        Sendet Multicast-Anfrage und wartet auf Leader-Antwort.

        Returns:
            (leader_ip, chat_port) oder None wenn kein Leader gefunden
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.settimeout(self.timeout)
        # TTL=1: Multicast bleibt im lokalen Netz (nicht ins Internet)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("b", 1))
        sock.bind(("", 0))  # Freien Port für Antwort reservieren

        discover_msg = json.dumps({"type": "DISCOVER_LEADER"}).encode()

        for attempt in range(1, self.retries + 1):
            print(f"[DISCOVERY] Suche Leader … (Versuch {attempt}/{self.retries})")

            # Multicast ins Netz schicken: "Wer ist der Leader?"
            sock.sendto(discover_msg, (MCAST_GROUP, MCAST_PORT))

            try:
                data, _ = sock.recvfrom(1024)
                msg = json.loads(data.decode())

                if msg.get("type") == "LEADER_RESPONSE":
                    leader_ip = msg["leader_ip"]
                    chat_port = msg["chat_port"]
                    print(f"[DISCOVERY] ✓ Leader gefunden: {leader_ip}:{chat_port}")
                    sock.close()
                    return leader_ip, chat_port

            except socket.timeout:
                print(f"[DISCOVERY] Keine Antwort – warte …")
                time.sleep(1)

        sock.close()
        print("[DISCOVERY] Kein Leader gefunden.")
        return None
