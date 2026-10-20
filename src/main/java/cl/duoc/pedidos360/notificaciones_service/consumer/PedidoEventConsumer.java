package cl.duoc.pedidos360.notificaciones_service.consumer;

import cl.duoc.pedidos360.notificaciones_service.dto.PedidoEvent;
import cl.duoc.pedidos360.notificaciones_service.service.NotificacionService;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class PedidoEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PedidoEventConsumer.class);

    private final NotificacionService notificacionService;
    private final ObjectMapper mapper;

    public PedidoEventConsumer(NotificacionService notificacionService, ObjectMapper mapper) {
        this.notificacionService = notificacionService;
        this.mapper = mapper;
    }

    public void procesar(ServiceBusReceivedMessageContext contexto) {
        ServiceBusReceivedMessage mensaje = contexto.getMessage();
        try {
            PedidoEvent evento = leerEvento(mensaje);
            notificacionService.enviar(evento);
            contexto.complete(); // ACK
            log.info("ACK del mensaje {}", mensaje.getMessageId());
        } catch (IllegalArgumentException e) {
            // Error no recuperable: va directo a la DLQ
            log.error("Mensaje {} enviado a DLQ: {}", mensaje.getMessageId(), e.getMessage());
            contexto.deadLetter(new DeadLetterOptions()
                    .setDeadLetterReason("MENSAJE_INVALIDO")
                    .setDeadLetterErrorDescription(e.getMessage()));
        } catch (Exception e) {
            // Error temporal: vuelve a la cola; al superar el maximo de entregas pasa a la DLQ
            log.warn("Error temporal en mensaje {} (entregas: {}), se reintenta: {}",
                    mensaje.getMessageId(), mensaje.getDeliveryCount(), e.getMessage());
            contexto.abandon();
        }
    }

    public void manejarError(ServiceBusErrorContext contexto) {
        log.error("Error del procesador de Service Bus: {}", contexto.getException().getMessage());
    }

    private PedidoEvent leerEvento(ServiceBusReceivedMessage mensaje) {
        try {
            return mapper.readValue(mensaje.getBody().toString(), PedidoEvent.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("JSON invalido: " + e.getMessage(), e);
        }
    }
}
