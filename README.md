# notificaciones-service

Microservicio **consumidor** del sistema Pedidos 360. Recibe los eventos `pedido.creado` publicados por el backend y genera la notificación.

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

## Estructura

```
config/    ServiceBusConsumerConfig   (cliente y procesador)
consumer/  PedidoEventConsumer        (ACK, reintentos y DLQ)
service/   NotificacionService        (lógica de negocio)
dto/       PedidoEvent
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
