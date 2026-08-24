import socket
from comun import descubrir_servidor, CLAVE, MSG_REGISTER, MSG_REG_RESP, MSG_METRIC
import psutil
import time

#incluir try catch?? hay que tener la cadena completa
ip, cpu_umbral, mem_umbral, tcp_port = descubrir_servidor()
#conexion tcp hacerla afuera

cliente_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cliente_tcp.connect((ip, tcp_port))
cliente_tcp.send((f"{MSG_REGISTER} {CLAVE} \n").encode('utf-8'))
respuesta = cliente_tcp.recv()

if respuesta.decode('utf-8').strip() == MSG_REG_RESP:
    while True:
        cpu = psutil.cpu_percent()
        memoria = psutil.virtual_memory().percent
        print(f"Enviando métrica CPU: {cpu}%")
        cliente_tcp.send((f"METRIC CPU {cpu}\n").encode('utf-8'))
        print(f"Enviando métrica MEM: {memoria}%")
        cliente_tcp.send((f"METRIC MEM {memoria}\n").encode('utf-8'))
        if cpu > cpu_umbral or memoria > mem_umbral:
            print("Límite de métrica alcanzado")
        if cpu > cpu_umbral:
            print(f"Alerta: CPU {cpu}% supera el umbral de {cpu_umbral}%")
            cliente_tcp.send((f"ALERT CPU {cpu}\n").encode('utf-8'))
        if memoria > mem_umbral:
            print(f"Alerta: MEM {memoria}% supera el umbral de {mem_umbral}%")
            cliente_tcp.send((f"ALERT MEM {memoria}\n").encode('utf-8'))
        time.sleep(15)