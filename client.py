import socket
import threading
import uuid
import time

from discovery import DiscoveryClient


class ChatClient:
    def __init__(self, username):
        self.username = username
        self.client_id = str(uuid.uuid4())

        self.server_host = None
        self.server_port = None

        self.socket = None
        self.running = False
        self.stop_requested = False
        self.reconnecting = False

        self.socket_lock = threading.Lock()

    def discover_leader(self):
        dc = DiscoveryClient(timeout=3.0, retries=5)
        result = dc.find_leader()

        if result is None:
            print("Kein Leader gefunden.")
            return False

        self.server_host, self.server_port = result
        return True

    def connect_to_leader(self):
        try:
            with self.socket_lock:
                if self.socket:
                    try:
                        self.socket.close()
                    except OSError:
                        pass

                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((self.server_host, self.server_port))

            self.running = True

            join_message = f"JOIN:{self.client_id}:{self.username}"
            self.send_raw(join_message)

            print(f"Verbunden mit Leader {self.server_host}:{self.server_port}")
            print(f"Client ID: {self.client_id}")
            return True

        except OSError as e:
            print(f"Verbindungsfehler: {e}")
            self.running = False
            return False

    def send_raw(self, message):
        with self.socket_lock:
            self.socket.sendall(message.encode("utf-8"))

    def reconnect_to_leader(self):
        if self.reconnecting or self.stop_requested:
            return False

        self.reconnecting = True
        self.running = False

        print("Suche neuen Leader...")

        while not self.stop_requested:
            if self.discover_leader() and self.connect_to_leader():
                print("Reconnect erfolgreich.")

                receive_thread = threading.Thread(
                    target=self.receive_messages,
                    daemon=True
                )
                receive_thread.start()

                self.reconnecting = False
                return True

            print("Kein Leader erreichbar. Neuer Versuch in 3 Sekunden...")
            time.sleep(3)

        self.reconnecting = False
        return False

    def receive_messages(self):
        while self.running and not self.stop_requested:
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

        if not self.stop_requested:
            self.reconnect_to_leader()

    def send_messages(self):
        while not self.stop_requested:
            try:
                message = input()

                if message.lower() == "/quit":
                    self.stop()
                    break

                full_message = f"MESSAGE:{self.client_id}:{self.username}:{message}"

                if not self.running:
                    print("Aktuell nicht verbunden. Warte auf Reconnect...")
                    if not self.reconnecting:
                        threading.Thread(target=self.reconnect_to_leader, daemon=True).start()

                    while not self.running and not self.stop_requested:
                        time.sleep(0.5)

                if self.stop_requested:
                    break

                try:
                    self.send_raw(full_message)
                except OSError:
                    print("Nachricht konnte nicht gesendet werden. Reconnect wird gestartet...")
                    self.running = False

                    if self.reconnect_to_leader():
                        self.send_raw(full_message)

            except KeyboardInterrupt:
                print("\nClient wird beendet.")
                self.stop()
                break

    def start(self):
        while not self.stop_requested:
            if self.discover_leader() and self.connect_to_leader():
                break

            print("Kein Leader erreichbar. Neuer Versuch in 3 Sekunden...")
            time.sleep(3)

        if self.stop_requested:
            return

        receive_thread = threading.Thread(target=self.receive_messages, daemon=True)
        receive_thread.start()

        self.send_messages()

    def stop(self):
        self.stop_requested = True
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