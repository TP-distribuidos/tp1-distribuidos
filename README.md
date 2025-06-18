# Sistema de Tolerancia a Fallos para Aplicación Distribuida

## Ejecucion

### Levantar nuevos clientes

Para levantar nuevos clientes usar el siguiente comando:
`docker compose -f docker-compose-test.yaml --profile manual up <client_container_name>`

Example
`docker compose -f docker-compose-test.yaml --profile manual up client4`

Esto permite:

- Mantener los servicios opcionales en el archivo principal de Compose.
- Iniciarlos solo cuando sea necesario.
- Asegurarse de que estén correctamente conectados a la misma red Docker que otros servicios.
- Mantener la consistencia de las variables de entorno y los montajes de volumen.

## Visión General

El sistema consiste en servicios worker que realizan las queries y servicios sentinel que monitorean a los workers bajo una arquitectura maestro-esclavo. Cuando se detectan fallos, el sistema reinicia automáticamente los componentes fallidos para mantener la disponibilidad del servicio.

```
                 ┌─────────────┐ Escucha      ┌─────────────┐  (Si el Maestro cae,
                 │             │ al Maestro   │             │   el Esclavo asume
         ┌───────► Sentinel 1  ◄─────────────►  Sentinel 2  │   el monitoreo de
         │       │  (Maestro)  │              │ (Esclavo)   │   los Workers)
         │       │             │              │             │
         │       └──────┬──────┘              └─────────────┘
         │              │
Responden│              │
Healthcheck             │ Monitorea
         │              │
         │              │ ───────────────────────
         │              ▼                        │
         │       ┌─────────────┐                 ▼
         │       │             │        ┌─────────────┐
         └───────│  Worker 1   │        │             │
                 │             │        │  Worker 2   │
                 └─────────────┘        │             │
                                        └─────────────┘
```

## Arquitectura

### Nodos

1. **Workers**: Nodos que realizan las queries y usan la interfaz **SentinelBeacon** para comunicarse con el sentinela maestro.
2. **Sentinelas**: Nodo de monitoreo que verifica la integridad de los workers através de una comunicación por socket. A la par los esclavos mmonitorean la integridad del maestro.

### Características del Diseño

#### 1. Arquitectura Maestro-Esclavo

Los sentinelas se monitorean entre sí utilizando una arquitectura maestro-esclavo a través de una conexión por socket TCP:

- Un sentinela actúa como maestro, responsable del monitoreo de todos los workers.
- Los otros sentinelas actúan como esclavos, recibiendo los broadcast de health check del maestro.
- Si algun esclavo falla, el sentinela maestro, lo detecta y lo revive.
- Si el maestro falla, los esclavos lo detectan e inician una nueva elección de líder.
- El nuevo maestro asume la responsabilidad de monitorear los workers y reinicia al sentinela maestro anterior

#### 2. Algoritmo de Elección de líder (Bully)

Se implementa un algoritmo tipo bully para la elección del maestro:

- Los sentinelas utilizan IDs únicos para determinar el liderazgo
- El ID más alto es elegido como el líder.
- Las elecciones se desencadenan cuando se detecta un fallo del maestro

## Detalles de Implementación

### Estructura del Código

#### 1. Clase Sentinel

El servicio principal de monitoreo que:

- Monitorea la salud de los workers
- Participa en la elección del maestro
- Reinicia workers y sentinelas fallidos

Métodos principales:

- `_check_worker_health(worker_host, worker_port)`: Verifica la salud del worker mediante conexión TCP
- `restart_worker(worker_host)`: Utiliza la API de Docker para reiniciar contenedores de worker fallidos
- `restart_sentinel(sentinel_hostname)`: Reinicia contenedores de sentinela fallidos
- `_initiate_election()`: Inicia el proceso de elección de maestro
- `_process_election_results()`: Determina el ganador de la elección y anuncia el nuevo maestro

#### 2. Clase SentinelBeacon

Una interfaz importada en cada worker, el cuál recibe las conexiones del sentinela maestro y le contesta los health checks:

- Escucha en un puerto designado
- Responde a las solicitudes de verificación de salud de los sentinelas

Métodos principales:

- `_run_sentinel_server()`: Ejecuta un servidor socket TCP para responder a los health checks.
- `_handle_sentinel_client(client_socket, addr)`: Procesa solicitudes entrantes de verificación de salud.

#### 3. Protocolo de Comunicación

Los sentinelas se comunican utilizando un protocolo de mensajería simple con tipos de mensajes:

- `ELECTION_START`: Inicia una elección de líder
- `ELECTION_RESPONSE`: Respuesta a un anuncio de elección de líder
- `LEADER_ANNOUNCEMENT`: Declara el maestro recién elegido
- `LEADER_HEARTBEAT`: Mensaje periódico del maestro a los esclavos
- `HEARTBEAT_ACK`: Confirmación de heartbeat de los esclavos
- `ID_ANNOUNCE`: Notificación de la presencia e ID de un sentinela para presentarse entre ellos.

### Monitoreo de Salud

#### Verificaciones de Salud de Workers

- Los sentinelas se conectan a los workers a través de un socket TCP (SentinelBeacon)
- Cada worker expone un puerto específico configurado en el docker compose. El cuál acepta las conexiones del Sentinela maestro.
- Los sentinelas envían un mensaje de heartbeat y esperan una respuesta.
- Un fallo de conexión o tiempo de espera agotado indica que el worker no está saludable, y debe de ser reiniciado.
  - Intenta reiniciar el container del worker utilizando la API de Docker.

#### Verificaciones de Salud de Sentinelas

- El maestro envía heartbeats periódicos a los sentinelas esclavos.
- Los esclavos escuchan los heartbeats del maestro.
- Si los heartbeats se detienen, los esclavos inician una nueva elección de líder.
- El nuevo líder reinicia el sentinela caido (previo líder).

### Implementación de los health checks

- Monitoreo secuencial de los workers que tiene asignado.
- Procesamiento concurrente de heartbeats de sentinela.
- Operaciones de verificación de salud no bloqueantes para los sentinelas esclavos.

## Configuración

El sistema de tolerancia a fallos se configura a través de variables de entorno en el archivo Docker Compose:

- `WORKER_HOSTS`: Lista de nombres de host de los workers a monitorear
- `WORKER_PORTS`: Lista de puertos para verificaciones de salud de los workers
- `CHECK_INTERVAL`: Frecuencia de las verificaciones de salud en segundos
- `SERVICE_NAME`: Nombre del servicio sentinela (usado para descubrimiento)
- `PEER_PORT`: Puerto para comunicación entre sentinelas
- `RESTART_ATTEMPTS`: Número de verificaciones fallidas antes del reinicio
- `RESTART_COOLDOWN`: Tiempo en segundos a esperar entre intentos de reinicio
- `COMPOSE_PROJECT_NAME`: Nombre del proyecto Docker Compose

## Conclusión

Este sistema de tolerancia a fallos proporciona una solución robusta para mantener la disponibilidad del servicio en una aplicación distribuida.

1. **Detecta y Recupera de Fallos**: El sistema puede detectar automáticamente y reiniciar workers fallidos, manteniendo la disponibilidad del servicio incluso durante fallos de componentes.

2. **Elimina Puntos Únicos de Fallo**: A través de la arquitectura maestro-esclavo y el mecanismo de elección, el sistema de monitoreo es resiliente frente a fallos.

3. **Asegura un Monitoreo Consistente**: El protocolo de elección de maestro garantiza que exactamente un sentinela sea responsable de las verificaciones de salud de los workers y su recuperación en cualquier momento.

4. **Ofrece Flexibilidad de Despliegue**: El sistema actualmente utiliza a Docker para reiniciar los nodos caidos, pero dado que la detección no depende de Docker en sí, se puede desacoplar el uso de Docker a favor de otra tecnología.
