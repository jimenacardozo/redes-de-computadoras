import socket, threading
from comun import descubrir_servidor, recv_line, enviar_linea, CLAVE, MSG_REGISTER, MSG_REG_RESP, MSG_GET_PROC, MSG_PROC, MSG_ALERT, MSG_METRIC
import psutil
import time

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

#incluir try catch?? hay que tener la cadena completa
ip, cpu_umbral, mem_umbral, tcp_port = descubrir_servidor()
#conexion tcp hacerla afuera

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
    hilo = threading.Thread(target=obtener_procesos, args=(cliente_tcp))
    hilo.start()

    while True:
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
else:
    print("Respuesta inesperada del servidor:", respuesta)

# TODO: CLOSE: cerrar el socket y el hilo de manera ordenada
