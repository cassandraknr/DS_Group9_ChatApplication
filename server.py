import socket
import threading


class ChatServer:
    # Server akzeptiert Verbidnungen über alle verfügbaren Netzwerkadressen des Rechners
    def __init__(self, host="0.0.0.0", port=5000):
        self.host = host
        self.port = port

        self.server_socket = None
        self.running = False

        # Dictionary speichert alle verbundenen Clients:
        self.clients = {}

    def start(self):
        # Erstellung eines Sockets
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Verhindert Fehler (address already in use), wenn man den Server neu startet
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()

        self.running = True
        print(f"Chat server started on {self.host}:{self.port}")

        while self.running:
            try:
                # Wartet bis ein Client verbinden möchte und akzeptiert dann
                client_socket, client_address = self.server_socket.accept()

                print(f"New connection from {client_address}")

                # Für jeden neuen Client wird ein eigener Thread erstellt, damit mehrere Clients gleichzeitig schreiben können
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address),
                    daemon=True
                )
                client_thread.start()

            except OSError:
                break

    def handle_client(self, client_socket, client_address):
        try:
            while self.running:
                data = client_socket.recv(1024)

                if not data:
                    break

                message = data.decode("utf-8")
                self.process_message(client_socket, message)

        except OSError:
            print(f"Connection lost: {client_address}")

        finally:
            self.remove_client(client_socket)

    def process_message(self, client_socket, message):
        parts = message.split(":", 3)

        message_type = parts[0]

        if message_type == "JOIN":
            self.handle_join(client_socket, parts)

        elif message_type == "MESSAGE":
            self.handle_chat_message(parts)

        else:
            print(f"Unknown message format: {message}")

    def handle_join(self, client_socket, parts):
        if len(parts) < 3:
            print("Invalid JOIN message.")
            return

        client_id = parts[1]
        username = parts[2]

        self.clients[client_socket] = {
            "client_id": client_id,
            "username": username
        }

        print(f"{username} joined the chat. Client ID: {client_id}")

        join_info = f"SERVER: {username} joined the chat."
        self.broadcast(join_info, sender_socket=client_socket)

    def handle_chat_message(self, parts):
        if len(parts) < 4:
            print("Invalid MESSAGE message.")
            return

        client_id = parts[1]
        username = parts[2]
        text = parts[3]

        chat_message = f"{username}: {text}"

        print(chat_message)
        self.broadcast(chat_message)

    def broadcast(self, message, sender_socket=None):
        disconnected_clients = []

        for client_socket in self.clients:
            if client_socket == sender_socket:
                continue

            try:
                client_socket.sendall(message.encode("utf-8"))

            except OSError:
                disconnected_clients.append(client_socket)

        for client_socket in disconnected_clients:
            self.remove_client(client_socket)

    def remove_client(self, client_socket):
        client_info = self.clients.get(client_socket)

        if client_info:
            username = client_info["username"]
            print(f"{username} disconnected.")
            del self.clients[client_socket]

            leave_info = f"SERVER: {username} left the chat."
            self.broadcast(leave_info)

        try:
            client_socket.close()
        except OSError:
            pass

    def stop(self):
        self.running = False

        for client_socket in list(self.clients.keys()):
            self.remove_client(client_socket)

        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass

        print("Server stopped.")


if __name__ == "__main__":
    server = ChatServer(host="0.0.0.0", port=5000)

    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()