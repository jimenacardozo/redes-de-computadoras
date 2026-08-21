import socket
from comun import descubrir_servidor


#incluir try catch?? hay que tener la cadena completa
ip, cpu_th, mem_th, tcp_port = descubrir_servidor()
#conexion tcp hacerla afuera
