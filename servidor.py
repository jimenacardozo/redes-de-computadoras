import socket
import threading
from collections import deque
from datetime import datetime
from comun import (
    recv_line, enviar_linea, parse_msg, UDP_PORT, CLAVE,
    MSG_DISCOVER, MSG_REGISTER, MSG_REG_RESP,
    MSG_ADMIN, MSG_ADMIN_RESP, MSG_LIST_AGENTS,
    MSG_GET_PROC, MSG_PROC, MSG_GET_METRIC, MSG_ALERT, 
    MSG_METRIC, MSG_ERROR, MSG_SERVER, MSG_AGENTS,
    MSG_MEASUREMENTS, MSG_END
)

HOST = "0.0.0.0"
TCP_PORT = 1234 # definir bien

UMBRAL_CPU = 100
UMBRAL_MEM = 100
TCP_REGISTRO_TIMEOUT = 10
AGENTE_COMUN_TIMEOUT = 45

ROL_SIN_REGISTRAR = "SIN_REGISTRAR"
ROL_COMUN = "COMUN"
ROL_ADMIN = "ADMIN"

COMANDOS_COMUN = {MSG_METRIC, MSG_PROC, MSG_ALERT, MSG_END}
COMANDOS_ADMIN = {MSG_LIST_AGENTS, MSG_GET_PROC, MSG_GET_METRIC, MSG_END}

# Estado compartido entre todos los hilos
agentes = {}
siguiente_id = 1
lock = threading.Lock()
lock_bitacora = threading.Lock() # protege las escrituras al log, separado del lock de agentes

def manejar_conexion_udp():
    servidor_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # AF_INET -> IPv4, SOCK_DGRAM -> UDP
    servidor_udp.bind((HOST, UDP_PORT))

    print(f"Servidor UDP escuchando en {HOST}:{UDP_PORT}")

    while True:
        data, addr = servidor_udp.recvfrom(1024) #Recibe datos del cliente UDP, 1024 bytes de tamaño máximo del buffer
        mensaje = data.decode('utf-8').strip() #Elimina espacios en blanco al inicio y saltos de linea
        print(f"Mensaje recibido de {addr}: {mensaje}")

        if mensaje == MSG_DISCOVER:
            #<umbral_cpu> <umbral_mem> <puerto_tcp>
            respuesta = f"{MSG_SERVER} {UMBRAL_CPU} {UMBRAL_MEM} {TCP_PORT}\n"
            servidor_udp.sendto(respuesta.encode('utf-8'), addr) #Envía la respuesta al cliente UDP
        else: 
            respuesta = f"Mensaje no reconocido: {mensaje}\n"
            servidor_udp.sendto(respuesta.encode('utf-8'), addr) #Envía la respuesta al cliente UDP


def registrar_agente_comun(conn, addr, argumentos):
    global siguiente_id

    if len(argumentos) != 1 or argumentos[0] != CLAVE:
        enviar_linea(conn, MSG_ERROR)
        print(f"Registro de agente comun rechazado desde {addr}")
        return None

    with lock:
        id_agente = siguiente_id
        siguiente_id += 1
        agentes[id_agente] = {
            "conn": conn,
            "addr": addr,
            "cpu": deque(maxlen=10),
            "mem": deque(maxlen=10),
            "procesos": None,
            "proc_event": None,
            "proc_lock": threading.Lock(), # serializa los pedidos GET_PROC hacia este agente
        }

    enviar_linea(conn, MSG_REG_RESP)
    print(f"Agente {id_agente} registrado desde {addr}")
    return id_agente


def registrar_admin(conn, addr, argumentos):
    if len(argumentos) != 1 or argumentos[0] != CLAVE:
        enviar_linea(conn, MSG_ERROR)
        print(f"Registro de admin rechazado desde {addr}")
        return False

    enviar_linea(conn, MSG_ADMIN_RESP)
    print(f"Admin conectado desde {addr}")
    return True


def manejar_mensaje_comun(conn, id_agente, comando, argumentos):
    if comando == MSG_METRIC:
        if len(argumentos) != 2:
            enviar_linea(conn, MSG_ERROR)
            return

        nombre_metrica, valor_texto = argumentos
        nombre_metrica = nombre_metrica.upper()
        if nombre_metrica not in {"CPU", "MEM"}:
            enviar_linea(conn, MSG_ERROR)
            return

        try:
            valor = float(valor_texto)
        except ValueError:
            enviar_linea(conn, MSG_ERROR)
            return

        with lock:
            agentes[id_agente][nombre_metrica.lower()].append(valor)
            cola_cpu = list(agentes[id_agente]["cpu"])
            cola_mem = list(agentes[id_agente]["mem"])

        print(f"[{nombre_metrica}] Nueva metrica: {valor}")
        print(f"Cola CPU: {cola_cpu}")
        print(f"Cola MEM: {cola_mem}\n")

    elif comando == MSG_PROC:
        procesos = " ".join(argumentos)
        with lock:
            agente = agentes.get(id_agente)
            if agente is not None:
                agente["procesos"] = procesos
                evento = agente.get("proc_event")

        if agente is None:
            enviar_linea(conn, MSG_ERROR)
            return

        if evento is not None:
            evento.set()

    elif comando == MSG_ALERT:
        if len(argumentos) != 2:
            enviar_linea(conn, MSG_ERROR)
            return

        nombre_metrica, valor_texto = argumentos
        nombre_metrica = nombre_metrica.upper()
        if nombre_metrica not in {"CPU", "MEM"}:
            enviar_linea(conn, MSG_ERROR)
            return

        try:
            valor = float(valor_texto)
        except ValueError:
            enviar_linea(conn, MSG_ERROR)
            return

        mensaje_alerta = (
            f"{MSG_ALERT} - {datetime.now()}: agente {id_agente} supera "
            f"el umbral de {nombre_metrica} con {valor}"
        )
        with lock_bitacora:
            with open("log.txt", "a") as bitacora: # Lo abre al archivo justo antes de escribir, y se cierra automáticamente al salir del bloque with
                bitacora.write(f"{mensaje_alerta}\n")
        print(mensaje_alerta)


def manejar_mensaje_admin(conn, comando, argumentos):
    if comando == MSG_LIST_AGENTS:
        if argumentos:
            enviar_linea(conn, MSG_ERROR)
            return

        with lock:
            ids = list(agentes.keys())
        respuesta = f"{MSG_AGENTS} {len(ids)}"
        if ids:
            respuesta += " " + " ".join(str(i) for i in ids)
        enviar_linea(conn, respuesta)

    elif comando == MSG_GET_PROC:
        if len(argumentos) != 1:
            enviar_linea(conn, MSG_ERROR)
            return

        try:
            id_solicitado = int(argumentos[0])
        except ValueError:
            enviar_linea(conn, MSG_ERROR)
            return

        with lock:
            agente = agentes.get(id_solicitado)

        if agente is None:
            enviar_linea(conn, MSG_ERROR)
            return

        with agente["proc_lock"]: # Bloquea el acceso a GET_PROC para este agente mientras se procesa la solicitud
            evento = threading.Event()
            agente["proc_event"] = evento
            socket_agente = agente["conn"]

            if not enviar_linea(socket_agente, MSG_GET_PROC):
                enviar_linea(conn, MSG_ERROR)
                return

            if not evento.wait(timeout=5):
                enviar_linea(conn, MSG_ERROR)
                return

            # Cuando llega aca se hizo event.set() en el hilo de cliente comun (PROC), y ya se guardo el resultado en agente["procesos"]
            resultado = agente["procesos"] 

        enviar_linea(conn, f"{MSG_PROC} {id_solicitado} {resultado}")

    elif comando == MSG_GET_METRIC:
        if len(argumentos) != 2:
            enviar_linea(conn, MSG_ERROR)
            return

        id_texto, nombre_metrica = argumentos
        nombre_metrica = nombre_metrica.upper()
        if nombre_metrica not in {"CPU", "MEM"}:
            enviar_linea(conn, MSG_ERROR)
            return

        try:
            id_solicitado = int(id_texto)
        except ValueError:
            enviar_linea(conn, MSG_ERROR)
            return

        with lock:
            agente = agentes.get(id_solicitado)
            if agente is not None:
                valores = list(agente[nombre_metrica.lower()])

        if agente is None:
            enviar_linea(conn, MSG_ERROR)
            return

        texto_valores = " ".join(str(valor) for valor in valores)
        respuesta = (
            f"{MSG_MEASUREMENTS} {id_solicitado} {nombre_metrica} "
            f"{len(valores)}"
        )
        if valores:
            respuesta += f" {texto_valores}"
        enviar_linea(conn, respuesta)


def conexion_tcp(conn, addr):
    id_agente = None
    rol = ROL_SIN_REGISTRAR
    buffer = b""  # acumula bytes hasta tener una linea completa
    conn.settimeout(TCP_REGISTRO_TIMEOUT)

    try:
        while True:
            mensaje, buffer = recv_line(conn, buffer)
            #no alcanza con cortar el de adentro de la funcion, porque sigo dentro del while
            if mensaje is None:
                break  # el cliente cerro la conexion

            comando, argumentos = parse_msg(mensaje)

            # El primer mensaje define el rol de la conexion. Despues, cada rol
            # solo puede usar los comandos que le corresponden.
            if rol == ROL_SIN_REGISTRAR:
                if comando == MSG_REGISTER:
                    nuevo_id = registrar_agente_comun(conn, addr, argumentos)
                    if nuevo_id is not None:
                        id_agente = nuevo_id
                        rol = ROL_COMUN
                        conn.settimeout(AGENTE_COMUN_TIMEOUT)
                elif comando == MSG_ADMIN:
                    if registrar_admin(conn, addr, argumentos):
                        rol = ROL_ADMIN
                        conn.settimeout(None)
                else:
                    enviar_linea(conn, MSG_ERROR)
                continue

            if comando == MSG_END:
                if argumentos:
                    enviar_linea(conn, MSG_ERROR)
                    continue
                break

            if rol == ROL_COMUN:
                if comando not in COMANDOS_COMUN:
                    enviar_linea(conn, MSG_ERROR)
                    continue
                manejar_mensaje_comun(conn, id_agente, comando, argumentos)
            elif rol == ROL_ADMIN:
                if comando not in COMANDOS_ADMIN:
                    enviar_linea(conn, MSG_ERROR)
                    continue
                manejar_mensaje_admin(conn, comando, argumentos)

    finally:
        # esto se ejecuta SIEMPRE al salir de la funcion: por END, por desconexion,
        # o por cualquier excepcion no esperada
        if id_agente is not None:
            with lock:
                agentes.pop(id_agente, None)
            print(f"Agente {id_agente} desconectado, espacio liberado")
        conn.close()

def manejar_conexion_tcp():
    servidor_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # AF_INET -> IPv4, SOCK_STREAM -> TCP
    servidor_tcp.bind((HOST, TCP_PORT))
    servidor_tcp.listen()

    print(f"Servidor TCP escuchando en {HOST}:{TCP_PORT}")

    while True:
        #defino los datos del cliente: conn = socket y addr = (ip, puerto). En el socket recibo ip, puerto y protocolo.
        conn, addr = servidor_tcp.accept() #Acepta conexiones entrantes
        print(f"Conexion TCP establecida con {addr}")
        #target la funcion que se ejecuta en el thread 
        #args los argumentos que se pasan a la funcion
        #Cada thread se corresponde con un cliente que se conecto al servidor
        threading.Thread(target=conexion_tcp, args=(conn, addr), daemon=True).start() 
        
hilo_udp = threading.Thread(target=manejar_conexion_udp, daemon=True) #Crea un hilo para manejar la conexion UDP
hilo_tcp = threading.Thread(target=manejar_conexion_tcp, daemon=True) #Crea un hilo para manejar la conexion TCP

hilo_udp.start() #Inicia el hilo UDP
print("Servidor UDP iniciado")
hilo_tcp.start() #Inicia el hilo TCP
print("Servidor TCP iniciado")

try:
    hilo_udp.join() #Espera a que el hilo UDP termine
    hilo_tcp.join() #Espera a que el hilo TCP termine
except KeyboardInterrupt:
    print("Servidor detenido por el usuario")
