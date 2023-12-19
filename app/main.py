import socket

HOST, PORT = "localhost", 6379


def main():
    server_socket = socket.create_server((HOST, PORT), reuse_port=True)
    conn, _ = server_socket.accept()  # wait for client
    while True:
        data = conn.recv(1024)
        if data:
            conn.sendall("+PONG\r\n".encode())
    conn.close()


if __name__ == "__main__":
    main()
