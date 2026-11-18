package cl.duoc.pedidos360.notificaciones_service.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** Consumidor Kafka. Solo se activa con app.kafka.enabled=true; el group.id lo define cada @KafkaListener. */
@Configuration
@EnableKafka
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true")
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${app.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /** Hilos consumidores por @KafkaListener. Con 3 particiones, 3 hilos leen una partición cada uno. */
    @Value("${app.kafka.concurrency:3}")
    private int concurrencia;

    /** read_committed en Docker; en Event Hubs (sin transacciones) se puede cambiar a read_uncommitted. */
    @Value("${app.kafka.isolation-level:read_committed}")
    private String isolationLevel;

    @Value("${app.kafka.assignor:org.apache.kafka.clients.consumer.CooperativeStickyAssignor}")
    private String assignor;

    /** Conexion segura opcional (p. ej. Azure Event Hubs: SASL_SSL + PLAIN). Si estan vacias, se conecta sin seguridad como en Docker. */
    @Value("${app.kafka.security-protocol:}")
    private String securityProtocol;

    @Value("${app.kafka.sasl-mechanism:}")
    private String saslMechanism;

    @Value("${app.kafka.sasl-jaas-config:}")
    private String saslJaasConfig;

    private void aplicarSeguridad(Map<String, Object> props) {
        if (!securityProtocol.isBlank()) props.put("security.protocol", securityProtocol);
        if (!saslMechanism.isBlank()) props.put("sasl.mechanism", saslMechanism);
        if (!saslJaasConfig.isBlank()) props.put("sasl.jaas.config", saslJaasConfig);
    }

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
        // Lotes pequeños: si un mensaje demora, no se supera max.poll.interval.ms y no se dispara un rebalanceo
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        // Si un consumidor deja de enviar latidos, el grupo lo da por caído y reparte sus particiones
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 5000);
        // En un rebalanceo solo se mueven las particiones necesarias, sin detener a todo el grupo
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignor);
        // Solo se leen mensajes confirmados por el broker
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        aplicarSeguridad(props);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /** Productor solo para enviar los mensajes fallidos al topico DLT. */
    @Bean
    public KafkaTemplate<String, String> dltKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        aplicarSeguridad(props);
        ProducerFactory<String, String> fabrica = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(fabrica);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            KafkaTemplate<String, String> dltTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Varios hilos por listener: reparten las particiones del tópico dentro de la misma instancia
        factory.setConcurrency(concurrencia);

        // Error temporal: 3 reintentos, 1 segundo entre cada uno. Mensaje invalido: no se reintenta.
        // Si no se puede procesar, el mensaje se envia al topico "<topico>.DLT" (misma particion) y se
        // registra en el log; el offset avanza para que un mensaje defectuoso no bloquee la particion.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dltTemplate,
                (registro, error) -> new TopicPartition(registro.topic() + ".DLT", registro.partition()));
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        errorHandler.setCommitRecovered(true);
        errorHandler.setRetryListeners((registro, error, intento) ->
                log.warn("Reintento {} topico={} particion={} offset={}: {}",
                        intento, registro.topic(), registro.partition(), registro.offset(), error.getMessage()));
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
