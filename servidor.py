import socket
import threading
from collections import deque
from comun import UDP_PORT, HOST, TCP_PORT, MSG_DISCOVER, MSG_REGISTER, CLAVE_SECRETA, MSG_END, MSG_METRIC, MSG_REG_RESP, MSG_ADMIN, MSG_ADMIN_RESP, MSG_LIST_AGENTS

MSG_SERVER = "SERVER"
UMBRAL_CPU = 100
UMBRAL_MEM = 100

# Estado compartido entre todos los hilos
agentes = {}
siguiente_id = 1
lock = threading.Lock()

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
    buffer = ""  # acumula bytes hasta tener una linea completa

    try:
        while True:
            data = conn.recv(1024) #recibo data
            if not data:
                break # el cliente cerro la conexion

            buffer += data.decode('utf-8')

            # puede haber 0, 1 o varios mensajes completos en el buffer
            while "\n" in buffer:
                print('entra al while')
                linea, buffer = buffer.split("\n", 1)  # separa el primer mensaje del resto
                mensaje = linea.strip()
                print('pasa el strip')
                print(f"{mensaje}")
                if mensaje == "":
                    print('mensaje vacio')
                    continue

                partes = mensaje.split(" ", 1) # Divido con el primer espacio que encuentre en un maximo de 2 partes
                comando = partes[0] # Me quedo con REGISTER/METRIC/END
                print(f"{comando}")
                if comando == MSG_REGISTER:
                    print('entra a msg_register')
                    if len(partes) < 2: # Puede venir un REGISTER sin nada, manejamos eso
                        conn.sendall(b"ERROR\n")
                        continue

                    clave = partes[1]
                    if clave != CLAVE_SECRETA:
                        conn.sendall(b"ERROR\n")
                        print('clave incorrecta')
                        continue

                    # Lock asegura que solo un hilo a la vez puede ejecutar el codigo de adentro
                    with lock:
                        print('entra al lock')
                        id_agente = siguiente_id
                        siguiente_id += 1  # sin 'global' arriba, esto tiraria UnboundLocalError
                        agentes[id_agente] = {
                            "conn": conn,
                            "addr": addr,
                            "cpu": deque(maxlen=10),
                            "mem": deque(maxlen=10),
                        }
                    conn.sendall(f"{MSG_REG_RESP}\n".encode('utf-8')) # envio el mensaje REG_RESP
                    print(f"Agente {id_agente} registrado desde {addr}")

                elif comando == MSG_ADMIN:
                    if len(partes) < 2:
                        conn.sendall(b"ERROR\n")
                        continue

                    clave = partes[1]
                    if clave != CLAVE_SECRETA:   # misma constante que en REGISTER
                        conn.sendall(b"ERROR\n")
                        continue

                    es_admin = True   # marca que esta conexion es un admin, no un agente comun
                    conn.sendall(f"{MSG_ADMIN_RESP}\n".encode('utf-8'))
                    print(f"Admin conectado desde {addr}")

                elif comando == MSG_LIST_AGENTS:
                    if not es_admin:            
                        conn.sendall(b"ERROR\n")
                        continue
                    
                    with lock:
                        ids = list(agentes.keys())
                    respuesta = f"AGENTS {len(ids)} " + " ".join(str(i) for i in ids)
                    conn.sendall(f"{respuesta}\n".encode('utf-8'))

                elif comando == MSG_METRIC:
                    print('recibo metrica')
                    if id_agente is None:
                        conn.sendall(b"ERROR\n")   # no registrado todavia
                        continue

                    # mensaje = "METRIC CPU 45.2" -> partes = ["METRIC", "CPU", "45.2"]
                    _, nombre_metrica, valor = mensaje.split(" ")
                    with lock:
                        agentes[id_agente][nombre_metrica.lower()].append(float(valor)) 
                        # Si hacemo float de un valor que no sea parseable a int esto de error capaz hay que hacer un chequeo?

                    # chequeo de umbral
                    umbral = UMBRAL_CPU if nombre_metrica == "CPU" else UMBRAL_MEM
                    if float(valor) > umbral:
                        print(f"[ALERTA] Agente {id_agente}: {nombre_metrica}={float(valor)} supera umbral {umbral}")
                        # aca tambien corresponderia registrar esto, segun pide la letra (ver como)

                elif comando == MSG_END:
                    break
            else:
                continue   # si el while interno termino SIN break, segui el while externo
            break   # si hubo break en el while interno (por END), rompe tambien el externo

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
print("Hilo UDP creado")
hilo_tcp = threading.Thread(target=manejar_conexion_tcp, daemon=True) #Crea un hilo para manejar la conexion TCP
print("Hilo TCP creado")

hilo_udp.start() #Inicia el hilo UDP
print("Hilo UDP iniciado")
hilo_tcp.start() #Inicia el hilo TCP
print("Hilo TCP iniciado")

hilo_udp.join() #Espera a que el hilo UDP termine
hilo_tcp.join() #Espera a que el hilo TCP termine


