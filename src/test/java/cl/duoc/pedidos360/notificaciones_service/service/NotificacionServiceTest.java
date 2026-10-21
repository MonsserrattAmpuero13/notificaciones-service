package cl.duoc.pedidos360.notificaciones_service.service;

import cl.duoc.pedidos360.notificaciones_service.dto.PedidoEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NotificacionServiceTest {

    private final NotificacionService service = new NotificacionService();

    @Test
    void procesaEventoValido() {
        PedidoEvent evento = new PedidoEvent(1L, "TRK-1", "Cliente", "Santiago", 1000);
        assertDoesNotThrow(() -> service.enviar(evento));
    }

    @Test
    void rechazaEventoSinTracking() {
        PedidoEvent evento = new PedidoEvent(1L, " ", "Cliente", "Santiago", 1000);
        assertThrows(IllegalArgumentException.class, () -> service.enviar(evento));
    }

    @Test
    void clienteErrorEsNoRecuperable() {
        PedidoEvent evento = new PedidoEvent(2L, "TRK-2", "ERROR", "Santiago", 1000);
        assertThrows(IllegalArgumentException.class, () -> service.enviar(evento));
    }

    @Test
    void clienteFallaEsTemporal() {
        PedidoEvent evento = new PedidoEvent(3L, "TRK-3", "FALLA", "Santiago", 1000);
        assertThrows(IllegalStateException.class, () -> service.enviar(evento));
    }
}
