package cl.duoc.pedidos360.notificaciones_service.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/** Consumidor Kafka. Solo se activa con app.kafka.enabled=true; el group.id lo define cada @KafkaListener. */
@Configuration
@EnableKafka
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true")
public class KafkaConsumerConfig {

    @Value("${app.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // El offset se confirma a mano, solo cuando el mensaje se proceso bien
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Un grupo nuevo lee desde el principio del topico
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Error temporal: 3 reintentos, 1 segundo entre cada uno. Mensaje invalido: no se reintenta.
        // Si igual no se puede procesar, se registra y se avanza para no bloquear la particion.
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 3L));
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        errorHandler.setCommitRecovered(true);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
