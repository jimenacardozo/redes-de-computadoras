import socket, threading
from comun import descubrir_servidor, recv_line, enviar_linea, CLAVE, MSG_REGISTER, MSG_REG_RESP, MSG_GET_PROC, MSG_PROC, MSG_ALERT, MSG_METRIC, MSG_END
import psutil
import time

termino_conexion = False

#TODO: Esta raro que hacemos recv_line en el hilo y en el main, pero esta ok porque en el main lo hace solo una vez y es para registrar al cliente
def obtener_procesos(cliente_tcp):
    buffer_hilo = b""  # acumula bytes hasta tener una linea completa
    while True:
        mensaje, buffer_hilo = recv_line(cliente_tcp, buffer_hilo)
        if mensaje is None:
            print("No se recibió mensaje del servidor en el hilo de procesos.")
            return
    
        if mensaje == MSG_GET_PROC:
            lista_procesos = []
            for proc in psutil.process_iter(['pid', 'name']):
                pid = proc.info['pid']
                name = proc.info['name']
                lista_procesos.append(f"{pid}:{name}")
                
            lista_procesos_str = ', '.join(lista_procesos)
            respuesta = f"{MSG_PROC} {lista_procesos_str}"
            enviar_linea(cliente_tcp, respuesta)

def escuchar_consola(cliente_tcp):
    global termino_conexion

    while True:
        comando = input("Ingrese un comando para enviar al servidor (o 'END' para termina r): ")
        if comando == 'END':
            enviar_linea(cliente_tcp, MSG_END)
            termino_conexion = True
            break

#TODO: incluir try catch?? hay que tener la cadena completa
ip, cpu_umbral, mem_umbral, tcp_port = descubrir_servidor()

cliente_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cliente_tcp.connect((ip, tcp_port))

buffer = b""  # acumula bytes hasta tener una linea completa
enviar_linea(cliente_tcp, f"{MSG_REGISTER} {CLAVE}")
respuesta, buffer = recv_line(cliente_tcp, buffer)
if respuesta is None:
    print("No se recibió respuesta del servidor.")
    cliente_tcp.close()
    exit(1)

#al final lo pusimos asi porque con esto ya validamos que este registrado el agente
if respuesta == MSG_REG_RESP:
    hilo_procesos = threading.Thread(target=obtener_procesos, args=(cliente_tcp,), daemon=True)
    hilo_procesos.start()
    
    hilo_consola = threading.Thread(target=escuchar_consola, args=(cliente_tcp, ), daemon=True)
    hilo_consola.start()

    while not termino_conexion:
        cpu = psutil.cpu_percent()
        memoria = psutil.virtual_memory().percent
        
        print(f"Enviando métrica CPU: {cpu}%")
        enviar_linea(cliente_tcp, f"{MSG_METRIC} CPU {cpu}")
        print(f"Enviando métrica MEM: {memoria}%")
        enviar_linea(cliente_tcp, f"{MSG_METRIC} MEM {memoria}")

        if cpu > cpu_umbral or memoria > mem_umbral:
            print("Límite de métrica alcanzado")

        if cpu > cpu_umbral:
            print(f"Alerta: CPU {cpu}% supera el umbral de {cpu_umbral}%")
            enviar_linea(cliente_tcp, f"{MSG_ALERT} CPU {cpu}")

        if memoria > mem_umbral:
            print(f"Alerta: MEM {memoria}% supera el umbral de {mem_umbral}%")
            enviar_linea(cliente_tcp, f"{MSG_ALERT} MEM {memoria}")

        time.sleep(15)

cliente_tcp.close()
print("Conexión cerrada.")