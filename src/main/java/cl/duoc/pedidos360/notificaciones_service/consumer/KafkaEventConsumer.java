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
        PedidoEvent evento = leer(registro.value(), PedidoEvent.class);
        notificacionService.enviar(evento);
        log.info("Kafka pedidos-creados particion={} offset={} clave={}",
                registro.partition(), registro.offset(), registro.key());
        ack.acknowledge(); // confirma el offset
    }

    @KafkaListener(topics = "${app.kafka.topic.alertas-stock}", groupId = "${app.kafka.group.alertas}")
    public void alertaStock(ConsumerRecord<String, String> registro, Acknowledgment ack) {
        AlertaStockEvent evento = leer(registro.value(), AlertaStockEvent.class);
        notificacionService.alertarStock(evento);
        log.info("Kafka alertas-stock particion={} offset={} clave={}",
                registro.partition(), registro.offset(), registro.key());
        ack.acknowledge();
    }

    private <T> T leer(String json, Class<T> tipo) {
        try {
            return mapper.readValue(json, tipo);
        } catch (Exception e) {
            throw new IllegalArgumentException("JSON invalido: " + e.getMessage(), e);
        }
    }
}
