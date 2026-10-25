package cl.duoc.pedidos360.notificaciones_service.consumer;

import cl.duoc.pedidos360.notificaciones_service.dto.AlertaStockEvent;
import cl.duoc.pedidos360.notificaciones_service.dto.PedidoEvent;
import cl.duoc.pedidos360.notificaciones_service.service.NotificacionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class KafkaEventConsumerTest {

    private NotificacionService notificacionService;
    private Acknowledgment ack;
    private KafkaEventConsumer consumer;

    @BeforeEach
    void preparar() {
        notificacionService = mock(NotificacionService.class);
        ack = mock(Acknowledgment.class);
        ObjectMapper mapper = JsonMapper.builder().build();
        consumer = new KafkaEventConsumer(notificacionService, mapper);
    }

    @Test
    void pedidoValidoSeProcesaYSeConfirmaElOffset() {
        String json = "{\"id\":1,\"tracking\":\"TRK-1\",\"cliente\":\"Ana\",\"destino\":\"Santiago\",\"total\":1000.0}";

        consumer.pedidoCreado(new ConsumerRecord<>("pedidos-creados", 0, 5L, "TRK-1", json), ack);

        verify(notificacionService).enviar(any(PedidoEvent.class));
        verify(ack).acknowledge();
    }

    @Test
    void jsonInvalidoLanzaErrorNoRecuperableYNoConfirmaElOffset() {
        ConsumerRecord<String, String> registro = new ConsumerRecord<>("pedidos-creados", 0, 6L, "X", "esto no es json");

        assertThrows(IllegalArgumentException.class, () -> consumer.pedidoCreado(registro, ack));

        verify(notificacionService, never()).enviar(any());
        verify(ack, never()).acknowledge();
    }

    @Test
    void alertaDeStockSeProcesaYSeConfirmaElOffset() {
        String json = "{\"productoId\":5,\"sku\":\"ASEO-001\",\"nombre\":\"Detergente\",\"stockActual\":3,\"stockMinimo\":10}";

        consumer.alertaStock(new ConsumerRecord<>("alertas-stock", 1, 2L, "ASEO-001", json), ack);

        verify(notificacionService).alertarStock(any(AlertaStockEvent.class));
        verify(ack).acknowledge();
    }
}
