import socket
from comun import descubrir_servidor, recv_line, enviar_linea, parse_msg, CLAVE, MSG_GET_METRIC, MSG_GET_PROC, MSG_ADMIN, MSG_ADMIN_RESP, MSG_LIST_AGENTS, MSG_END

ip, cpu_umbral, mem_umbral, tcp_port = descubrir_servidor()

cliente_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cliente_tcp.connect((ip, tcp_port))

buffer = b""  # acumula bytes hasta tener una linea completa
enviar_linea(cliente_tcp, f"{MSG_ADMIN} {CLAVE}")

respuesta, buffer = recv_line(cliente_tcp, buffer)
if respuesta is None:
    print("No se recibió respuesta del servidor.")
    cliente_tcp.close()
    exit(1)

if respuesta == MSG_ADMIN_RESP:
    ids_agentes = []
    buffer_comando = b""

    while True:
        print("Comandos disponibles:")
        print(" L -> Listar agentes conectados")
        print(" M <x> <CPU|MEM> -> Ver métrica del agente x (ej: M 1 CPU)")
        print(" P <x> -> Ver procesos del agente x (ej: P 2)")
        print(" Para salir, escriba 'END'")
        comando = input("Escriba un comando: ")
        partes = comando.strip().split()
        if not partes:
            print("Comando vacío")

        elif partes[0] == "L":
            enviar_linea(cliente_tcp, MSG_LIST_AGENTS)
            respuesta, buffer_comando = recv_line(cliente_tcp, buffer_comando)
            if respuesta is None:
                print("No se recibió respuesta del servidor.")
                break

            comando, argumentos = parse_msg(respuesta)
            ids_agentes = argumentos[1:]  # Ignorar el primer elemento que es el comando
            print("Agentes conectados:")
            for i, id_agente in enumerate(ids_agentes, start=1):
                print(f"{i} - Agente: {id_agente}")

        elif partes[0] == "M" and len(partes) == 3:
            if not ids_agentes:
                print("Primero debe listar los agentes conectados con el comando L.")
                continue

            try:
                ordinal = int(partes[1])  # Convertir a entero
            except ValueError:
                print("El primer argumento debe ser un número entero que representa el agente.")
                continue
            
            if ordinal < 1 or ordinal > len(ids_agentes):
                print(f"Agente inválido. Debe ser un número entre 1 y {len(ids_agentes)}.")
                continue
            
            tipo_metrica = partes[2].upper() # partes[2] = CPU o MEM
            agente_id = ids_agentes[ordinal - 1]  # Convertir a índice

            if tipo_metrica not in ["CPU", "MEM"]:
                print("Tipo de métrica inválido. Use CPU o MEM.")
            else:
                enviar_linea(cliente_tcp, f"{MSG_GET_METRIC} {agente_id} {tipo_metrica}")
                respuesta, buffer_comando = recv_line(cliente_tcp, buffer_comando)
                if respuesta is None:
                    print("No se recibió respuesta del servidor.")
                    break
                    
                print(respuesta.strip())

        elif partes[0] == "P" and len(partes) == 2:
            if not ids_agentes:
                print("Primero debe listar los agentes conectados con el comando L.")
                continue
            
            try:
                ordinal = int(partes[1])  # Convertir a entero
            except ValueError:
                print("El primer argumento debe ser un número entero que representa el agente.")
                continue
            
            if ordinal < 1 or ordinal > len(ids_agentes):
                print(f"Agente inválido. Debe ser un número entre 1 y {len(ids_agentes)}.")
                continue

            agente_id = ids_agentes[ordinal - 1]  # Convertir a índice
            enviar_linea(cliente_tcp, f"{MSG_GET_PROC} {agente_id}")
            respuesta, buffer_comando = recv_line(cliente_tcp, buffer_comando)
            if respuesta is None:
                print("No se recibió respuesta del servidor.")
                break

            print(respuesta.strip())

        elif partes[0] == MSG_END:
            enviar_linea(cliente_tcp, f"{MSG_END}")
            cliente_tcp.close()
            print("Conexión cerrada.")
            break
            
        else:
            print("Comando inválido.")

cliente_tcp.close()
print("Conexión cerrada.")
