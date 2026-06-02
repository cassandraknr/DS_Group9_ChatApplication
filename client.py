import socket
import threading
import uuid

class ChatClient:
    #Server Adresse und Port sind auf den eigenen Rechner gesetzt und müssen später durch UDP Discovery ersetzt werden.
    def __init__(self, username, server_host = "127.0.0.1", server_port = 5000):
        #Für die Anzeige des Usernames
        self.username = username
        #Für die technische Identität des Clients
        self.client_id = str(uuid.uuid4())

        self.server_host = server_host
        self.server_port = server_port
        self.socket = None
        self.running = False

    def discover_leader(self):
        #Placeholder for UDP Discovery
        return self.server_host, self.server_port

    def connect_to_leader(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.server_host, self.server_port))
            self.running = True

            # 
            join_message = f"JOIN:{self.client_id}:{self.username}"
            self.socket.sendall(join_message.encode("utf-8"))

            print(f"Connected to leader at {self.server_host}:{self.server_port}")
            print(f"Client ID: {self.client_id}")

        except ConnectionRefusedError:
            print("Could not connect to leader server. Is the server running?")
            self.running = False

        except OSError as error:
            print(f"Connection error: {error}")
            self.running = False

    def receive_messages(self):
        while self.running:
            try:
                message = self.socket.recv(1024).decode("utf-8")

                if not message:
                    print("Connection to server closed.")
                    self.running = False
                    break

                print(message)

            except OSError:
                print("Lost connection to leader server.")
                self.running = False
                break

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
                print("Could not send message. Connection lost.")
                self.running = False
                break

    def start(self):
        self.discover_leader()
        self.connect_to_leader()

        if not self.running:
            return

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

        print("Client stopped.")

if __name__ == "__main__":
    username = input("Enter username: ")

    client = ChatClient(username)
    client.start()