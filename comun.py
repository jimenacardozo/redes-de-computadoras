import socket
import time

UDP_PORT = 6021 # usado por descubrir_servidor() aca abajo y por servidor.py

DISCOVERY_TIMEOUT = 3 # segundos que se espera respuesta SERVER
DISCOVERY_RETRIES = 3
TCP_RECV_BUFSIZE = 4096
TCP_TIMEOUT = 10 # segundos para operaciones TCP que esperan respuesta

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
CLAVE = "redes2026grupo21"


def recv_line(socket, buffer) -> tuple[str | None, bytes]:
    while b"\n" not in buffer:
        try:
            datos = socket.recv(TCP_RECV_BUFSIZE)
        except OSError:
            # Timeout, conexion reseteada, etc.: se trata igual que un cierre normal
            return None, None
        if not datos:
            # El otro extremo cerró la conexión
            return None, None
        buffer += datos
    linea, buffer = buffer.split(b"\n", 1)
    return linea.decode("utf-8"), buffer


def enviar_linea(socket, mensaje) -> bool:
    """Envia un mensaje de texto terminado en un salto de linea.
    Devuelve True si se pudo enviar, False si el socket ya no esta disponible."""
    try:
        socket.sendall((mensaje + "\n").encode("utf-8"))
        return True
    except OSError:
        return False


def parse_msg(linea: str) -> tuple[str, list[str]]:
    """
    Separa un mensaje en (tipo, [argumentos]).
    Ej: "SERVER 80 90 5000" -> ("SERVER", ["80", "90", "5000"])
    """
    partes = linea.strip().split()
    if not partes:
        return "", []
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
