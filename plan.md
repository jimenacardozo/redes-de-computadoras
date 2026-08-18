Buena pregunta, y ojo que "común" en este obligatorio es ambiguo: está el módulo comun.py (el archivo compartido que evita repetir código) y está el cliente_comun.py (el agente que reporta métricas). Voy a explicarte el módulo comun.py, que es lo primero que van a escribir y el cimiento de todo. Si querías el agente común, avisame y lo vemos aparte.

Para qué existe comun.py

La letra dice que los clientes "podrán tener un archivo incluido común para no repetir código". La idea es meter ahí todo lo que tiene que ver con cómo se hablan los programas —no con qué hacen— para que servidor, cliente común y cliente admin lo importen y hablen exactamente el mismo idioma. Si el formato de un mensaje cambia, lo cambiás en un solo lado.

Tiene tres responsabilidades, de menor a mayor dificultad:

1. Las constantes del protocolo (lo fácil)

Los nombres de los mensajes y los parámetros del sistema, como constantes en vez de strings sueltos desperdigados por el código. DISCOVER, SERVER, REGISTER, METRIC, ALERT, GET_PROC, END, etc., más cosas como el número de grupo, el puerto UDP 60NN, la clave secreta, el intervalo de 15 segundos, timeouts. La ganancia es que si escribís RESGITER mal en un lado, no hay error hasta runtime; si usás una constante, salta enseguida. Esto es media hora de trabajo.

2. El framing: leer hasta el \n (lo importante de verdad)

Este es el concepto del módulo y el que más cuesta. Acordate del punto de la defensa: TCP entrega un flujo de bytes, no mensajes. Cuando el común manda tres mensajes seguidos:

METRIC CPU 37\n
METRIC MEM 61\n
ALERT CPU 95\n

del otro lado un recv(4096) te puede devolver cualquier corte: los tres juntos en un solo pedazo, o METRIC CPU 37\nMETRIC M (dos mensajes y medio), o incluso METR y en el siguiente recv te llega IC CPU 37\n. No podés asumir "un recv = un mensaje". Por eso todos los mensajes terminan en \n: es el separador que te dice dónde termina uno y empieza el otro.

La solución es acumular en un buffer y cortar por \n. La lógica es esta (pseudocódigo, para que lo tipeen ustedes):

buffer = b""                         # bytes que todavía no forman una línea entera
def recibir_linea(sock, buffer):
    while b"\n" not in buffer:       # mientras no tenga un mensaje completo...
        datos = sock.recv(4096)
        if not datos:                # el otro cerró la conexión
            → manejar cierre
        buffer += datos              # ...sigo acumulando
    linea, resto = buffer.split(b"\n", 1)   # corto en el PRIMER \n
    buffer = resto                   # lo que sobró queda para la próxima llamada
    return linea.decode(), buffer

Lo clave: el buffer es estado que sobrevive entre llamadas. Por eso conviene envolverlo en una clasecita tipo LectorDeLineas(sock) que se guarde su propio buffer, o pasarlo de vuelta como en el pseudo. Y muy importante para la defensa: cada conexión TCP necesita su propio buffer. Un buffer global rompería todo, porque el servidor tiene muchas conexiones a la vez y se le mezclarían los flujos. Esto es exactamente lo mismo que vieron en la Parte 1 con las dos sesiones telnet que no se mezclaban: cada conexión es su propio flujo independiente.

Para el lado de enviar, el par de esto es una función enviar_linea(sock, mensaje) que le agrega el \n, codifica a bytes y usa sendall (no send: send puede mandar solo una parte de los bytes y devolverte cuántos mandó; sendall insiste hasta mandar todo).

3. Armar y desarmar mensajes (parseo)

Helpers para pasar de "estructura de datos" a "string de red" y viceversa. Armar es fácil: f"METRIC {metrica} {valor}". Desarmar es partir la línea: el primer token es el tipo, el resto son argumentos.

partes = linea.split()      # "METRIC CPU 37" -> ["METRIC", "CPU", "37"]
tipo = partes[0]            # "METRIC"
args = partes[1:]           # ["CPU", "37"]

La trampa está en los mensajes con estructura interna, como PROC 12:bash,15:python. Ahí el split() por espacio te da ["PROC", "12:bash,15:python"], y el segundo token todavía hay que parsearlo aparte: primero por , para separar los procesos, después cada par por : para separar PID y nombre. Vale la pena tener un helper dedicado para ese formato.

Lo que no va en comun.py: la lógica de negocio. Medir CPU con psutil, la concurrencia del servidor, el loop de comandos del admin, el almacenamiento de los 10 valores... eso vive en cada componente. El módulo común es solo el "idioma", no el "cerebro".

Por eso en el plan lo marqué como "chico pero crítico" y como lo primero que hacen las tres juntas: si el framing está mal, fallan los tres programas a la vez y encima con bugs intermitentes horribles de depurar (a veces anda, a veces el mensaje llega cortado). Media tarde bien invertida acá les ahorra días después.

Un detalle de las reglas: esto que te expliqué es concepto y esqueleto para que lo entiendan; el código lo escriben ustedes y declaran que usaron IA para entender el framing. Así llegan a la defensa pudiendo explicar cada línea.

¿Seguimos con el cliente_comun.py (el agente, con el tema de los dos hilos), o querés que profundicemos el parseo o el buffer con algún caso más?