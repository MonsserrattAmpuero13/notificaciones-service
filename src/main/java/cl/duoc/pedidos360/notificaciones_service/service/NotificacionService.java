package cl.duoc.pedidos360.notificaciones_service.service;

import cl.duoc.pedidos360.notificaciones_service.dto.AlertaStockEvent;
import cl.duoc.pedidos360.notificaciones_service.dto.PedidoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class NotificacionService {

    private static final Logger log = LoggerFactory.getLogger(NotificacionService.class);

    public void enviar(PedidoEvent evento) {
        if (evento.tracking() == null || evento.tracking().isBlank()) {
            throw new IllegalArgumentException("Evento sin tracking");
        }
        // Casos de prueba para la demo
        if ("ERROR".equalsIgnoreCase(evento.cliente())) {
            throw new IllegalArgumentException("Cliente de prueba: error no recuperable");
        }
        if ("FALLA".equalsIgnoreCase(evento.cliente())) {
            throw new IllegalStateException("Cliente de prueba: falla temporal");
        }
        log.info("Notificacion enviada: pedido {} de {} con destino {}",
                evento.tracking(), evento.cliente(), evento.destino());
    }

    public void alertarStock(AlertaStockEvent evento) {
        if (evento.sku() == null || evento.sku().isBlank()) {
            throw new IllegalArgumentException("Alerta sin SKU");
        }
        log.warn("ALERTA DE STOCK: {} ({}) tiene {} unidades; el minimo es {}",
                evento.nombre(), evento.sku(), evento.stockActual(), evento.stockMinimo());
    }
}
