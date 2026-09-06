import socket, threading
from comun import descubrir_servidor, recv_line, enviar_linea, CLAVE, TCP_TIMEOUT, MSG_REGISTER, MSG_REG_RESP, MSG_GET_PROC, MSG_PROC, MSG_ALERT, MSG_METRIC, MSG_END
import psutil
import time

termino_conexion = False
lock_envio = threading.Lock() # evita que dos hilos escriban al mismo tiempo sobre cliente_tcp

def enviar_linea_segura(cliente_tcp, mensaje) -> bool:
    with lock_envio:
        return enviar_linea(cliente_tcp, mensaje)

def obtener_procesos(cliente_tcp):
    buffer_hilo = b""  # acumula bytes hasta tener una linea completa
    while True:
        mensaje, buffer_hilo = recv_line(cliente_tcp, buffer_hilo)
        if mensaje is None:
            print("Conexión cerrada, dejando de escuchar procesos.")
            return
    
        if mensaje == MSG_GET_PROC:
            lista_procesos = []
            for proc in psutil.process_iter(['pid', 'name']):
                pid = proc.info['pid']
                name = proc.info['name']
                lista_procesos.append(f"{pid}:{name}")
                
            lista_procesos_str = ', '.join(lista_procesos)
            respuesta = f"{MSG_PROC} {lista_procesos_str}"
            if not enviar_linea_segura(cliente_tcp, respuesta):
                print("No se pudo enviar la respuesta de procesos, conexión perdida.")
                return

def escuchar_consola(cliente_tcp):
    global termino_conexion

    while True:
        comando = input("Ingrese comando 'END' para terminar: \n\n")
        if comando == 'END':
            enviar_linea_segura(cliente_tcp, MSG_END)
            termino_conexion = True
            break

try:
    ip, cpu_umbral, mem_umbral, tcp_port = descubrir_servidor()
except Exception as e:
    print(f"Error al descubrir el servidor: {e}")
    exit(1)

cliente_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cliente_tcp.settimeout(TCP_TIMEOUT)
try:
    cliente_tcp.connect((ip, tcp_port))
except OSError as e:
    print(f"Error al conectar por TCP con el servidor: {e}")
    cliente_tcp.close()
    exit(1)

buffer = b""  # acumula bytes hasta tener una linea completa
if not enviar_linea(cliente_tcp, f"{MSG_REGISTER} {CLAVE}"):
    print("No se pudo enviar el registro al servidor.")
    cliente_tcp.close()
    exit(1)

respuesta, buffer = recv_line(cliente_tcp, buffer)
if respuesta is None:
    print("No se recibió respuesta del servidor.")
    cliente_tcp.close()
    exit(1)

if respuesta == MSG_REG_RESP:
    cliente_tcp.settimeout(None)

    hilo_procesos = threading.Thread(target=obtener_procesos, args=(cliente_tcp,), daemon=True)
    hilo_procesos.start()
    
    hilo_consola = threading.Thread(target=escuchar_consola, args=(cliente_tcp, ), daemon=True)
    hilo_consola.start()

    while not termino_conexion:
        cpu = psutil.cpu_percent()
        memoria = psutil.virtual_memory().percent

        print(f"Enviando métrica CPU: {cpu}%")
        if not enviar_linea_segura(cliente_tcp, f"{MSG_METRIC} CPU {cpu}"):
            print("Se perdió la conexión con el servidor.")
            break
        
        print(f"Enviando métrica MEM: {memoria}%")
        if not enviar_linea_segura(cliente_tcp, f"{MSG_METRIC} MEM {memoria}"):
            print("Se perdió la conexión con el servidor.")
            break

        if cpu > cpu_umbral or memoria > mem_umbral:
            print("Límite de métrica alcanzado")

        if cpu > cpu_umbral:
            print(f"Alerta: CPU {cpu}% supera el umbral de {cpu_umbral}%")
            enviar_linea_segura(cliente_tcp, f"{MSG_ALERT} CPU {cpu}")

        if memoria > mem_umbral:
            print(f"Alerta: MEM {memoria}% supera el umbral de {mem_umbral}%")
            enviar_linea_segura(cliente_tcp, f"{MSG_ALERT} MEM {memoria}")

        time.sleep(15)

cliente_tcp.close()
print("Conexión cerrada.")