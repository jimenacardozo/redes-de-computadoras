import socket
import threading
from collections import deque
from datetime import datetime
from comun import (
    recv_line, UDP_PORT, CLAVE,
    MSG_DISCOVER, MSG_REGISTER, MSG_REG_RESP,
    MSG_ADMIN, MSG_ADMIN_RESP, MSG_LIST_AGENTS,
    MSG_GET_PROC, MSG_PROC, MSG_GET_METRIC, MSG_ALERT, 
    MSG_METRIC, MSG_ERROR
)

HOST = "0.0.0.0"
TCP_PORT = 1234 # definir bien

MSG_SERVER = "SERVER"
MSG_AGENTS = "AGENTS"
MSG_MEASUREMENTS = "MEASUREMENTS"
MSG_END = "END"

UMBRAL_CPU = 100
UMBRAL_MEM = 100

# Estado compartido entre todos los hilos
agentes = {}
siguiente_id = 1
lock = threading.Lock()
bitacora = open("log.txt", "w") #con w crea el archivo si no existe y lo vacia si ya existe. 

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

def conexion_tcp(conn, addr):
    global siguiente_id
    id_agente = None
    es_admin = False
    buffer = b""  # acumula bytes hasta tener una linea completa

    try:
        while True:
            mensaje, buffer = recv_line(conn, buffer)
            #no alcanza con cortar el de adentro de la funcion, porque sigo dentro del while
            if mensaje is None:
                break  # el cliente cerro la conexion

            partes = mensaje.split(" ", 1) # Divido con el primer espacio que encuentre en un maximo de 2 partes
            comando = partes[0] # Me quedo con REGISTER/METRIC/END
            if comando == MSG_REGISTER:
                if len(partes) < 2: # Puede venir un REGISTER sin nada, manejamos eso
                    conn.sendall(f"{MSG_ERROR}\n".encode('utf-8'))
                    continue

                clave = partes[1]
                if clave != CLAVE:
                    conn.sendall(f"{MSG_ERROR}\n".encode('utf-8'))
                    print(f"Registro rechazado: clave incorrecta desde {addr}")
                    continue

                # Lock asegura que solo un hilo a la vez puede ejecutar el codigo de adentro
                with lock:
                    id_agente = siguiente_id
                    siguiente_id += 1  # sin 'global' arriba, esto tiraria UnboundLocalError
                    agentes[id_agente] = {
                        "conn": conn,
                        "addr": addr,
                        "cpu": deque(maxlen=10),
                        "mem": deque(maxlen=10),
                        "procesos": None,
                        "proc_event": None,
                    }
                conn.sendall(f"{MSG_REG_RESP}\n".encode('utf-8')) # envio el mensaje REG_RESP
                print(f"Agente {id_agente} registrado desde {addr}")

            elif comando == MSG_ADMIN:
                if len(partes) < 2:
                    conn.sendall(f"{MSG_ERROR}\n".encode('utf-8'))
                    continue

                clave = partes[1]
                if clave != CLAVE:   # misma constante que en REGISTER
                    conn.sendall(f"{MSG_ERROR}\n".encode('utf-8'))
                    continue

                es_admin = True   # marca que esta conexion es un admin, no un agente comun
                conn.sendall(f"{MSG_ADMIN_RESP}\n".encode('utf-8'))
                print(f"Admin conectado desde {addr}")

            elif comando == MSG_LIST_AGENTS:
                if not es_admin:            
                    conn.sendall(f"{MSG_ERROR}\n".encode('utf-8'))
                    continue
                
                with lock:
                    ids = list(agentes.keys())
                respuesta = f"{MSG_AGENTS} {len(ids)} " + " ".join(str(i) for i in ids)
                conn.sendall(f"{respuesta}\n".encode('utf-8'))

            elif comando == MSG_METRIC:
                if id_agente is None:
                    conn.sendall(f"{MSG_ERROR}\n".encode('utf-8'))   # no registrado todavia
                    continue

                # mensaje = "METRIC CPU 45.2" -> partes = ["METRIC", "CPU", "45.2"]
                _, nombre_metrica, valor = mensaje.split(" ")
                with lock:
                    agentes[id_agente][nombre_metrica.lower()].append(float(valor))
                    # TODO: Si hacemo float de un valor que no sea parseable a int esto de error capaz hay que hacer un chequeo?
                    cola_cpu = list(agentes[id_agente]["cpu"])
                    cola_mem = list(agentes[id_agente]["mem"])
                print(f"[{nombre_metrica}] Nueva métrica: {valor}")
                print(f"Cola CPU: {cola_cpu}")
                print(f"Cola MEM: {cola_mem}\n")

                # chequeo de umbral
                # TODO: Deberiamos hacer un chequeo de que nombre_metrica sea CPU o MEM ???? 
                umbral = UMBRAL_CPU if nombre_metrica == "CPU" else UMBRAL_MEM
                if float(valor) > umbral:
                    print(f"ALERTA: agente {id_agente} supera el umbral de {nombre_metrica} ({float(valor)} > {umbral})")
                    #TODO: falta escribir bitacora, aca tambien corresponderia registrar esto, segun pide la letra (ver como)
            
            elif comando == MSG_GET_PROC:
                id_solicitado = partes[1] # id de agente comun que el admin quiere consultar

                with lock: # Bloqueo para acceder a la estructura compartida de agentes
                    agente = agentes.get(int(id_solicitado))
                    if agente is None:
                        conn.sendall(f"{MSG_ERROR}: agente {id_solicitado} no encontrado\n".encode('utf-8'))
                        continue

                    evento = threading.Event() # Creo un evento para sincronizar el hilo del admin con el hilo del agente comun
                    agente["proc_event"] = evento
                    agente["procesos"] = None
                    socket = agente["conn"]

                # Envio al agente comun el mensaje GET_PROC para que me devuelva la lista de procesos
                # TODO: esto puede fallar? 
                socket.sendall(f"{MSG_GET_PROC}\n".encode('utf-8')) 

                llego = evento.wait(timeout=5)  # espera hasta 5s la respuesta del agente
                if not llego:
                    conn.sendall(f"{MSG_ERROR}\n".encode("utf-8"))
                    continue

                with lock:
                    resultado = agente["procesos"]

                conn.sendall( f"{MSG_PROC} {id_solicitado} {resultado}\n".encode('utf-8')) # Respondo al admin con la lista de procesos del agente comun

            elif comando == MSG_PROC:
                with lock: # Bloqueo para acceder a la estructura compartida de agentes
                    agente = agentes.get(int(id_agente))
                    if agente is None: #agente no esta
                        continue

                    evento = agente.get("proc_event")
                    if len(partes) > 1:
                        agente["procesos"] = partes[1]
                    else:
                        agente["procesos"] = ""

                if evento is not None:
                    evento.set()   # despierta al hilo del admin que estaba esperando
            
            elif comando == MSG_GET_METRIC:
                if not es_admin:  # TODO: esto es necesario? 
                    conn.sendall(f"{MSG_ERROR}\n".encode('utf-8'))
                    continue
                
                if len(partes) < 2: # TODO: esto es necesario? 
                    conn.sendall(f"{MSG_ERROR}\n".encode('utf-8'))
                    continue

                id_solicitado, nombre_metrica = partes[1].split(" ")

                # TODO: tendriamos que manejar si nombre_metrica es cualquier cosa?
                # TODO: Tendriamos que manajear si id_solicitado no es un numero o no existe en agentes?
                
                with lock:
                    agente = agentes.get(int(id_solicitado))
                    if agente is None:
                        conn.sendall(f"{MSG_ERROR}: agente {id_solicitado} no encontrado\n".encode('utf-8'))
                        continue

                    cola = agente[nombre_metrica.lower()] #TODO: deberiamos guardar una copia?
                
                texto_valores = " ".join(str(v) for v in cola)
                conn.sendall(f"{MSG_MEASUREMENTS} {id_solicitado} {nombre_metrica} {len(cola)} {texto_valores}\n".encode('utf-8'))

            elif comando == MSG_ALERT:
                # mensaje = "ALERT CPU 45.2" -> partes = ["ALERT", "CPU", "45.2"]
                _, nombre_metrica, valor = mensaje.split(" ")
                mensaje_alerta = f"{MSG_ALERT} - {datetime.now()}: agente {id_agente} supera el umbral de {nombre_metrica} con {valor}"
                bitacora.write(f"{mensaje_alerta}\n")
                print(f"{mensaje_alerta}")   

            elif comando == MSG_END:
                break # Sale a fuera del while y cierra la conexion TCP pasando por finally

            else:
                conn.sendall(f"{MSG_ERROR}\n".encode('utf-8'))
                print(f"Comando no reconocido: {mensaje}")

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
    bitacora.close() #Cierra el archivo de bitacora  
    print("Servidor detenido por el usuario")
