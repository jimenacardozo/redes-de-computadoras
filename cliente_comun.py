import socket
from comun import descubrir_servidor, CLAVE, MSG_REGISTER
import psutil
import time

#incluir try catch?? hay que tener la cadena completa
ip, cpu, mem, tcp_port = descubrir_servidor()
#conexion tcp hacerla afuera

cliente_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cliente_tcp.connect((ip, tcp_port))
cliente_tcp.send((f"{MSG_REGISTER} {CLAVE} \n").encode('utf-8'))
 
while True:
    cpu = psutil.cpu_percent()
    memoria = psutil.virtual_memory()
    cliente_tcp.send((f"METRIC CPU {cpu}").encode('utf-8'))
    cliente_tcp.send((f"METRIC MEM {memoria}").encode('utf-8'))
    time.sleep(15)