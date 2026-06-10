"""
client.py – Chat-Client mit echter Dynamic Discovery
=====================================================

Änderung gegenüber dem Original:
  discover_leader() war ein Placeholder → jetzt echte Multicast-Discovery
  über DiscoveryClient aus discovery.py

Alles andere (connect, send, receive) bleibt unverändert.
"""

import socket
import threading
import uuid

# ── NEU: Discovery importieren ────────────────────────────────────
from discovery import DiscoveryClient


class ChatClient:

    def __init__(self, username):
        self.username  = username
        self.client_id = str(uuid.uuid4())  # Eindeutige UUID dieses Clients

        # Adresse wird NICHT mehr fest gesetzt –
        # discover_leader() füllt diese Felder dynamisch
        self.server_host = None
        self.server_port = None

        self.socket  = None
        self.running = False

    # ══════════════════════════════════════════════════════════════
    # DISCOVERY – Placeholder wurde durch echte Multicast-Logik ersetzt
    #
    # Vorher:
    #   def discover_leader(self):
    #       # Placeholder for UDP Discovery
    #       return self.server_host, self.server_port
    #
    # Jetzt:
    #   DiscoveryClient sendet Multicast → Leader antwortet →
    #   server_host + server_port werden gesetzt
    # ══════════════════════════════════════════════════════════════
    def discover_leader(self) -> bool:
        """
        Sucht den Leader per Multicast-Discovery.
        Setzt self.server_host und self.server_port wenn gefunden.

        Returns:
            True  → Leader gefunden
            False → kein Leader erreichbar
        """
        dc     = DiscoveryClient(timeout=3.0, retries=5)
        result = dc.find_leader()  # Blockiert bis Leader antwortet oder alle Versuche erschöpft

        if result is None:
            print("Kein Leader gefunden. Bitte zuerst Knoten starten.")
            return False

        # Leader gefunden → Adresse speichern
        self.server_host, self.server_port = result
        return True

    # ══════════════════════════════════════════════════════════════
    # VERBINDUNG ZUM LEADER – unverändert gegenüber Original
    # ══════════════════════════════════════════════════════════════
    def connect_to_leader(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.server_host, self.server_port))
            self.running = True

            # Beim Leader anmelden
            join_message = f"JOIN:{self.client_id}:{self.username}"
            self.socket.sendall(join_message.encode("utf-8"))

            print(f"Verbunden mit Leader {self.server_host}:{self.server_port}")
            print(f"Client ID: {self.client_id}")

        except ConnectionRefusedError:
            print("Verbindung abgelehnt. Läuft der Server?")
            self.running = False
        except OSError as e:
            print(f"Verbindungsfehler: {e}")
            self.running = False

    # ══════════════════════════════════════════════════════════════
    # NACHRICHTEN EMPFANGEN – unverändert
    # ══════════════════════════════════════════════════════════════
    def receive_messages(self):
        while self.running:
            try:
                message = self.socket.recv(1024).decode("utf-8")
                if not message:
                    print("Verbindung zum Server getrennt.")
                    self.running = False
                    break
                print(message)
            except OSError:
                print("Verbindung zum Leader verloren.")
                self.running = False
                break

    # ══════════════════════════════════════════════════════════════
    # NACHRICHTEN SENDEN – unverändert
    # ══════════════════════════════════════════════════════════════
    def send_messages(self):
        while self.running:
            try:
                message = input()
                if message.lower() == "/quit":
                    self.stop()
                    break
                full_message = f"MESSAGE:{self.client_id}:{self.username}:{message}"
                self.socket.sendall(full_message.encode("utf-8"))
            except OSError:
                print("Nachricht konnte nicht gesendet werden.")
                self.running = False
                break

    # ══════════════════════════════════════════════════════════════
    # START – Discovery läuft jetzt wirklich
    # ══════════════════════════════════════════════════════════════
    def start(self):
        # Schritt 1: Leader per Multicast finden
        if not self.discover_leader():
            return  # Kein Leader → abbrechen

        # Schritt 2: Direkt mit Leader verbinden
        self.connect_to_leader()
        if not self.running:
            return

        # Schritt 3: Empfangen im Hintergrund, Senden im Vordergrund
        receive_thread = threading.Thread(target=self.receive_messages, daemon=True)
        receive_thread.start()
        self.send_messages()

    def stop(self):
        self.running = False
        if self.socket:
            try:
                self.socket.close()
            except OSError:
                pass
        print("Client beendet.")


if __name__ == "__main__":
    username = input("Benutzername eingeben: ")
    client = ChatClient(username)
    client.start()
