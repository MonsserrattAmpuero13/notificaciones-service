package cl.duoc.pedidos360.notificaciones_service.consumer;

import cl.duoc.pedidos360.notificaciones_service.dto.AlertaStockEvent;
import cl.duoc.pedidos360.notificaciones_service.dto.PedidoEvent;
import cl.duoc.pedidos360.notificaciones_service.service.NotificacionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Consumidores Kafka. Cada @KafkaListener pertenece a un consumer group:
 *  - notificaciones-group lee "pedidos-creados": si se levantan 2 copias del servicio, las 3 particiones se reparten entre ellas.
 *  - alertas-stock-group lee "alertas-stock": otro grupo, tiene su propio offset.
 */
@Component
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true")
public class KafkaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventConsumer.class);

    private final NotificacionService notificacionService;
    private final ObjectMapper mapper;

    public KafkaEventConsumer(NotificacionService notificacionService, ObjectMapper mapper) {
        this.notificacionService = notificacionService;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "${app.kafka.topic.pedidos-creados}", groupId = "${app.kafka.group.notificaciones}")
    public void pedidoCreado(ConsumerRecord<String, String> registro, Acknowledgment ack) {
        PedidoEvent evento = leer(registro, PedidoEvent.class);
        exigir(evento.tracking() != null && !evento.tracking().isBlank(), registro, "falta el tracking");
        exigir(evento.cliente() != null && !evento.cliente().isBlank(), registro, "falta el cliente");
        notificacionService.enviar(evento);
        log.info("Kafka pedidos-creados particion={} offset={} clave={}",
                registro.partition(), registro.offset(), registro.key());
        ack.acknowledge(); // confirma el offset
    }

    @KafkaListener(topics = "${app.kafka.topic.alertas-stock}", groupId = "${app.kafka.group.alertas}")
    public void alertaStock(ConsumerRecord<String, String> registro, Acknowledgment ack) {
        AlertaStockEvent evento = leer(registro, AlertaStockEvent.class);
        exigir(evento.sku() != null && !evento.sku().isBlank(), registro, "falta el sku");
        notificacionService.alertarStock(evento);
        log.info("Kafka alertas-stock particion={} offset={} clave={}",
                registro.partition(), registro.offset(), registro.key());
        ack.acknowledge();
    }

    /** Convierte el JSON al DTO. Si no se puede, el mensaje es defectuoso: no se reintenta y va al topico DLT. */
    private <T> T leer(ConsumerRecord<String, String> registro, Class<T> tipo) {
        if (registro.value() == null || registro.value().isBlank()) {
            throw invalido(registro, "mensaje vacio");
        }
        try {
            return mapper.readValue(registro.value(), tipo);
        } catch (Exception e) {
            throw invalido(registro, "JSON invalido (" + e.getMessage() + ")");
        }
    }

    private void exigir(boolean condicion, ConsumerRecord<String, String> registro, String motivo) {
        if (!condicion) throw invalido(registro, motivo);
    }

    private IllegalArgumentException invalido(ConsumerRecord<String, String> registro, String motivo) {
        String detalle = "Mensaje descartado topico=" + registro.topic() + " particion=" + registro.partition()
                + " offset=" + registro.offset() + " clave=" + registro.key() + ": " + motivo;
        log.error(detalle);
        return new IllegalArgumentException(detalle);
    }
}
