import socket
from comun import descubrir_servidor, recv_line, CLAVE, MSG_GET_METRIC, MSG_GET_PROC

ip, cpu_umbral, mem_umbral, tcp_port = descubrir_servidor()

cliente_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cliente_tcp.connect((ip, tcp_port))

buffer = b""  # acumula bytes hasta tener una linea completa
cliente_tcp.send((f"ADMIN {CLAVE}\n").encode('utf-8'))
respuesta, buffer = recv_line(cliente_tcp, buffer)
if respuesta is None:
    print("No se recibió respuesta del servidor.")
    cliente_tcp.close()
    exit(1)

if respuesta == "ADMIN_RESP":
    print("Comandos disponibles:")
    print(" L -> Listar agentes conectados")
    print(" M <x> <CPU|MEM> -> Ver métrica del agente x (ej: M 1 CPU)")
    print(" P <x> -> Ver procesos del agente x (ej: P 2)")
    comando = input("Escriba un comando: ")
    partes = comando.strip().split(" ")
    if not partes:
        print("Comando vacío")

    elif partes[0] == "L":
        cliente_tcp.send((f"LIST_AGENTS\n").encode('utf-8'))
        respuesta = cliente_tcp.recv(2048)
        print(respuesta.decode('utf-8').strip())

    elif partes[0] == "M" and len(partes) == 3:
        agente_id, tipo_metrica = partes[1], partes[2].upper()
        if tipo_metrica not in ["CPU", "MEM"]:
            print("Tipo de métrica inválido. Use CPU o MEM.")
        else:
            cliente_tcp.send((f"{MSG_GET_METRIC} {agente_id} {tipo_metrica}\n").encode('utf-8'))
            respuesta = cliente_tcp.recv(2048)
            print(respuesta.decode('utf-8').strip())

    elif partes[0] == "P" and len(partes) == 2:
        agente_id = partes[1]
        cliente_tcp.send((f"{MSG_GET_PROC} {agente_id}\n").encode('utf-8'))
        respuesta = cliente_tcp.recv(2048)
        print(respuesta.decode('utf-8').strip())
        
    else:
        print("Comando inválido.")