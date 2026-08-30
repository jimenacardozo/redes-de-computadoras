import socket
from comun import descubrir_servidor, recv_line, enviar_linea, CLAVE, MSG_GET_METRIC, MSG_GET_PROC, MSG_ADMIN, MSG_ADMIN_RESP, MSG_LIST_AGENTS

ip, cpu_umbral, mem_umbral, tcp_port = descubrir_servidor()

cliente_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cliente_tcp.connect((ip, tcp_port))

buffer = b""  # acumula bytes hasta tener una linea completa
enviar_linea(cliente_tcp, f"{MSG_ADMIN} {CLAVE}")

while True:
    respuesta, buffer = recv_line(cliente_tcp, buffer)
    if respuesta is None:
        print("No se recibió respuesta del servidor.")
        cliente_tcp.close()
        exit(1)

    if respuesta == MSG_ADMIN_RESP:
        print("Comandos disponibles:")
        print(" L -> Listar agentes conectados")
        print(" M <x> <CPU|MEM> -> Ver métrica del agente x (ej: M 1 CPU)")
        print(" P <x> -> Ver procesos del agente x (ej: P 2)")
        comando = input("Escriba un comando: ")
        partes = comando.strip().split(" ")
        buffer_comando = b""
        if not partes:
            print("Comando vacío")

        elif partes[0] == "L":
            enviar_linea(cliente_tcp, MSG_LIST_AGENTS)
            respuesta, buffer_comando = recv_line(cliente_tcp, buffer_comando)
            if respuesta is None:
                print("No se recibió respuesta del servidor.")
                #TODO: cerrar el socket y el hilo de manera ordenada??

            print(respuesta.decode('utf-8').strip())

        elif partes[0] == "M" and len(partes) == 3:
            agente_id, tipo_metrica = partes[1], partes[2].upper()
            if tipo_metrica not in ["CPU", "MEM"]:
                print("Tipo de métrica inválido. Use CPU o MEM.")
            else:
                enviar_linea(cliente_tcp, f"{MSG_GET_METRIC} {agente_id} {tipo_metrica}")
                respuesta, buffer_comando = recv_line(cliente_tcp, buffer_comando)
                if respuesta is None:
                    print("No se recibió respuesta del servidor.")
                    #TODO: cerrar el socket y el hilo de manera ordenada??
                    
                print(respuesta.decode('utf-8').strip())

        elif partes[0] == "P" and len(partes) == 2:
            agente_id = partes[1]
            #TODO: como se cual es el id de agente?? partes[1] es el indice nomas
            #se puede llamar al list agents y volver a convertirlo?
            #hacemos que sea necesario pedir antes la L para guardar la lista antes> o en el medio me la pueden cambiar?
            enviar_linea(cliente_tcp, f"{MSG_GET_PROC} {agente_id}")
            respuesta, buffer_comando = recv_line(cliente_tcp, buffer_comando)
            if respuesta is None:
                print("No se recibió respuesta del servidor.")
                #TODO: cerrar el socket y el hilo de manera ordenada??
                
            print(respuesta.decode('utf-8').strip())
            
        else:
            print("Comando inválido.")

    # TODO: CLOSE: cerrar el socket y el hilo de manera ordenada
