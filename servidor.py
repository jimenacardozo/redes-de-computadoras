import socket
import threading
from comun import UDP_PORT, HOST, TCP_PORT, MSG_DISCOVER

MSG_SERVER = "SERVER"
UMBRAL_CPU = 100
UMBRAL_MEM = 100

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

def manejar_conexion_tcp():
    servidor_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # AF_INET -> IPv4, SOCK_STREAM -> TCP
    servidor_tcp.bind((HOST, TCP_PORT))
    servidor_tcp.listen()

    print(f"Servidor TCP escuchando en {HOST}:{TCP_PORT}")

    while True:
        #defino los datos del cliente: conn = socket y addr = (ip, puerto). En el socket recibo ip, puerto y protocolo.
        conn, addr = servidor_tcp.accept() #Acepta conexiones entrantes
        print(f"Conexión TCP establecida con {addr}")
        #target la funcion que se ejecuta en el thread 
        #args los argumentos que se pasan a la funcion
        #Cada thread se corresponde con un cliente que se conecto al servidor
        #threading.Thread(target=monitorizacion, args=(conn, addr), daemon=True).start() 
        

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


