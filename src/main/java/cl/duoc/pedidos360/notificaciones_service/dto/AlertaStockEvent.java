package cl.duoc.pedidos360.notificaciones_service.dto;

/** Evento del topico "alertas-stock": un producto quedo en o bajo su stock minimo. */
public record AlertaStockEvent(Long productoId, String sku, String nombre, int stockActual, int stockMinimo) {}
