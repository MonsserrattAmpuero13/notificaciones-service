package cl.duoc.pedidos360.notificaciones_service.config;

import cl.duoc.pedidos360.notificaciones_service.consumer.PedidoEventConsumer;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServiceBusConsumerConfig {

    @Value("${spring.cloud.azure.servicebus.connection-string}")
    private String connectionString;

    @Value("${app.servicebus.topic}")
    private String topic;

    @Value("${app.servicebus.subscription}")
    private String subscription;

    @Bean(initMethod = "start", destroyMethod = "close")
    public ServiceBusProcessorClient pedidosProcessor(PedidoEventConsumer consumer) {
        return new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .processor()
                .topicName(topic)
                .subscriptionName(subscription)
                .disableAutoComplete() // ACK manual
                .processMessage(consumer::procesar)
                .processError(consumer::manejarError)
                .buildProcessorClient();
    }
}
