# notificaciones-service

Microservicio **consumidor** del sistema Pedidos 360. Recibe los eventos `pedido.creado` publicados por el backend y genera la notificación. Escucha dos vías en paralelo: Azure Service Bus (flujo original) y Apache Kafka (EP5, se activa con `KAFKA_ENABLED=true`).

## Flujo

1. El backend publica el evento en el topic `pedidos-topic`.
2. Este servicio escucha la suscripción `notificaciones-sub`.
3. Procesa el mensaje y confirma (ACK) con `complete()`.

## Manejo de errores y DLQ

| Situación | Acción |
|---|---|
| Procesamiento correcto | `complete()` (ACK explícito) |
| Error **no recuperable** (JSON inválido, evento sin tracking, cliente `ERROR`) | `deadLetter()` directo a la DLQ, con motivo `MENSAJE_INVALIDO`, y registro en el log |
| Error **temporal** (cliente `FALLA`) | `abandon()`: el mensaje vuelve a la cola y se reintenta; al superar el máximo de entregas (3) pasa a la DLQ |

El ACK automático está desactivado (`disableAutoComplete()`), por lo que cada mensaje se confirma de forma explícita.

## Kafka (EP5)

Los consumidores están en `consumer/KafkaEventConsumer` y usan `@KafkaListener`. La lógica de negocio sigue en `NotificacionService`.

| Tópico | Consumer group | Qué hace |
|---|---|---|
| `pedidos-creados` | `notificaciones-group` | Genera la notificación del pedido |
| `alertas-stock` | `alertas-stock-group` | Registra la alerta de stock bajo el mínimo |

- **Configuración:** `config/KafkaConsumerConfig` (brokers, deserializers, confirmación manual del offset, lectura desde el inicio para un grupo nuevo).
- **Balanceo:** si se levantan dos copias del servicio con el mismo grupo, Kafka reparte las 3 particiones entre ellas. Si una se cae, las particiones pasan a la que sigue viva.
- **Validación del mensaje:** se descarta (con log del tópico, partición y offset) si está vacío, si no es JSON válido o si faltan datos obligatorios (`tracking`, `cliente` o `sku`).
- **Reintentos:** error temporal → 3 reintentos con 1 segundo de espera, cada uno registrado en el log. Mensaje inválido → no se reintenta.
- **Mensajes fallidos:** se envían al tópico `<tópico>.DLT` (misma partición) y el offset avanza, para que un mensaje defectuoso no bloquee el resto.

Crear los tópicos DLT antes de probar (con `kafka-admin-service` en el puerto 8083):

```
curl -X POST http://localhost:8083/api/kafka/topics -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"pedidos-creados.DLT","partitions":3,"replicationFactor":3}'
curl -X POST http://localhost:8083/api/kafka/topics -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"alertas-stock.DLT","partitions":3,"replicationFactor":3}'
```

| Variable | Uso |
|---|---|
| `KAFKA_ENABLED` | `true` para activar los consumidores de Kafka (por defecto `false`) |
| `KAFKA_BOOTSTRAP_SERVERS` | Brokers (por defecto `localhost:9092,localhost:9093,localhost:9094`) |

Para probar un mensaje defectuoso, publicar desde Kafka UI (http://localhost:8090) en `pedidos-creados` un texto que no sea JSON: aparece el error en el log y el mensaje llega a `pedidos-creados.DLT`.

## Estructura

```
config/    ServiceBusConsumerConfig   (cliente y procesador)
           KafkaConsumerConfig        (cliente Kafka, reintentos y DLT)
consumer/  PedidoEventConsumer        (ACK, reintentos y DLQ de Service Bus)
           KafkaEventConsumer         (@KafkaListener)
service/   NotificacionService        (lógica de negocio)
dto/       PedidoEvent, AlertaStockEvent
```

Los nombres del topic y la suscripción están centralizados en `src/main/resources/application.yml`.

## Configuración

| Variable | Uso |
|---|---|
| `SERVICEBUS_CONNECTION_STRING` | Cadena de conexión del namespace (obligatoria, no se sube al repositorio) |

Puerto: `8081`.

## Ejecución

```
export SERVICEBUS_CONNECTION_STRING="<cadena de conexión>"
./mvnw spring-boot:run
```

## Cómo probar la DLQ

Crear un pedido desde la aplicación (o con la API) usando como **cliente**:

- `ERROR`: el mensaje va directo a la DLQ.
- `FALLA`: se reintenta y luego pasa a la DLQ.

Los mensajes se ven en Azure: namespace → topic `pedidos-topic` → suscripción `notificaciones-sub` → *Service Bus Explorer* → pestaña *Dead-letter*.

## Pruebas

```
./mvnw test
```

Las pruebas de `NotificacionService` no requieren conexión a Azure.

## Equivalencias RabbitMQ → Azure Service Bus

| Concepto de la pauta (RabbitMQ) | Equivalente en Azure Service Bus |
|---|---|
| Exchange | Topic |
| Queue | Suscripción |
| Binding | Suscripción asociada al topic |
| DLQ | Dead-letter queue integrada |
| `@RabbitListener` | `ServiceBusProcessorClient` |
| ACK / NACK | `complete()` / `abandon()` / `deadLetter()` |
