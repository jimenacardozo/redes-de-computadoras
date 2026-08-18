import socket
import threading
import random

HOST = "127.0.0.1"
PORT = 2026


def uno_mas_para_atender(conn, addr):
    while True:
        datos = conn.recv(1024)
        if not datos:
            break

        comando, cantidad = datos.decode().split()
        cantidad = int(cantidad)

        if comando == "par":
            numero = random.randrange(0, 10000, 2)
        else:
            numero = random.randrange(1, 10000, 2)

        respuesta = " ".join([str(numero)] * cantidad)
        conn.sendall(respuesta.encode())

    conn.close()


servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
servidor.bind((HOST, PORT))
servidor.listen()

while True:
    conn, addr = servidor.accept()
    threading.Thread(target=uno_mas_para_atender, args=(conn, addr), daemon=True).start()