import socket
import time

HOST = "0.0.0.0"
UDP_PORT = 6021
TCP_PORT = 1234 # definir bien

DISCOVERY_TIMEOUT = 3 # segundos que se espera respuesta SERVER
DISCOVERY_RETRIES = 3
TCP_RECV_BUFSIZE = 4096

# UDP
MSG_DISCOVER = "DISCOVER"
MSG_SERVER = "SERVER"

# TCP - comun
MSG_REGISTER = "REGISTER"
MSG_REG_RESP = "REG_RESP"
MSG_METRIC = "METRIC"
MSG_GET_PROC = "GET_PROC"
MSG_PROC = "PROC"
MSG_ALERT = "ALERT"

# TCP - admin
MSG_ADMIN = "ADMIN"
MSG_ADMIN_RESP = "ADMIN_RESP"
MSG_LIST_AGENTS = "LIST_AGENTS"
MSG_AGENTS = "AGENTS"
MSG_GET_METRIC = "GET_METRIC"
MSG_MEASUREMENTS = "MEASUREMENTS"

# Comunes a ambos
MSG_ERROR = "ERROR"
MSG_END = "END"

# Clave secreta
CLAVE_SECRETA = "redes2026grupo21"

def send_msg(sock: socket.socket, texto: str) -> None:
    """Envía un mensaje de protocolo, agregando el terminador '\n'."""
    sock.sendall((texto + "\n").encode("utf-8"))


class LineReader:
    """
    Envuelve un socket TCP para leer mensajes completos (terminados en \n),
    manteniendo un buffer entre llamadas porque un recv() puede traer
    datos de más de un mensaje, o de un mensaje incompleto.

    Uso: un LineReader por conexión (cada agente conectado al servidor
    tiene el suyo).
    """

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self._buffer = b""

    def recv_line(self) -> str | None:
        """
        Devuelve el próximo mensaje completo (sin el \n), o None si el
        socket se cerró (peer hizo close / envió FIN).
        """
        while b"\n" not in self._buffer:
            datos = self.sock.recv(TCP_RECV_BUFSIZE)
            if not datos:
                # El otro extremo cerró la conexión
                return None
            self._buffer += datos

        linea, self._buffer = self._buffer.split(b"\n", 1)
        return linea.decode("utf-8")


def parse_msg(linea: str) -> tuple[str, list[str]]:
    """
    Separa un mensaje en (tipo, [argumentos]).
    Ej: "SERVER 80 90 5000" -> ("SERVER", ["80", "90", "5000"])
    """
    partes = linea.strip().split(" ")
    return partes[0], partes[1:]


# Descubrimiento UDP (lado cliente: lo usan cliente_comun y cliente_admin)

def descubrir_servidor():
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # Instancia UDP
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1) # Permite broadcast
    udp_sock.settimeout(DISCOVERY_TIMEOUT) # Timeout para recvfrom()

    try:
        for intento in range(1, DISCOVERY_RETRIES + 1):
            try:
                udp_sock.sendto(
                    (MSG_DISCOVER + "\n").encode("utf-8"),
                    ("255.255.255.255", UDP_PORT),
                ) # Envía broadcast DISCOVER
                print(f"Enviando broadcast DISCOVER a {UDP_PORT}")

                # Espera respuesta, en datos queda el mensaje y en addr la dirección del emisor
                datos, addr = udp_sock.recvfrom(TCP_RECV_BUFSIZE) 
                print(f"Respuesta recibida de {addr}: {datos.decode('utf-8')}")
                tipo, args = parse_msg(datos.decode("utf-8"))

                if tipo != MSG_SERVER or len(args) != 3:
                    # Respuesta inesperada: se descarta y se reintenta.
                    continue # se saltea lo que esta abajo pero sigue con la prox iteracion del for
                print(f"Tipo: {tipo}, Args: {args}")

                umbral_cpu, umbral_mem, puerto_tcp = args
                server_ip = addr[0]
                print(f"Server IP: {server_ip}, Umbral CPU: {umbral_cpu}, Umbral MEM: {umbral_mem}, Puerto TCP: {puerto_tcp}")
                return server_ip, int(umbral_cpu), int(umbral_mem), int(puerto_tcp)

            except socket.timeout:
                print(f"[descubrimiento] intento {intento} sin respuesta, reintentando...")
                continue

        #raise sirve para lanzar una excepcion, en este caso TimeoutError, con un mensaje de error.
        #se crea solo cuando terminan los 3 intentos del for y no se hizo return
        raise TimeoutError("No se pudo descubrir al servidor tras varios intentos.") 

    finally:
        udp_sock.close() # Cierra el socket UDP al terminar, ya sea por éxito o por excepción.
