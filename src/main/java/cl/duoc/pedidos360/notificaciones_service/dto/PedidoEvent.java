package cl.duoc.pedidos360.notificaciones_service.dto;

public record PedidoEvent(Long id, String tracking, String cliente, String destino, double total) {}
