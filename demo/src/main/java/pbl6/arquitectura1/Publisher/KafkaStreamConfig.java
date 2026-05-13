package pbl6.arquitectura1.Publisher;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * KafkaStreamConfig - Declara todos los exchanges y colas antes de arrancar el sistema.
 * Idempotente: se puede ejecutar varias veces sin error.
 */
public class KafkaStreamConfig {

    public static final String EXCHANGE_STREAM  = "stream_garraioa";
    public static final String EXCHANGE_FANOUT  = "fanout_garraioa";
    public static final String EXCHANGE_EMAITZA = "emaitza_garraioa";
    public static final String EXCHANGE_LAINOA  = "lainoa";          // ← añadido

    public static final String QUEUE_TAREA   = "tarea";
    public static final String QUEUE_KOTXEA  = "kotxea";
    public static final String QUEUE_PUBLIKO = "publikoa";
    public static final String QUEUE_KP      = "k_p";
    public static final String QUEUE_EMAITZA = "emaitza";

    private ConnectionFactory factory;

    public KafkaStreamConfig() {
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
    }

    public void configurar() {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // 1. STREAM (direct) → Q:tarea
            channel.exchangeDeclare(EXCHANGE_STREAM, "direct", true);
            channel.queueDeclare(QUEUE_TAREA, true, false, false, null);
            channel.queueBind(QUEUE_TAREA, EXCHANGE_STREAM, QUEUE_TAREA);
            System.out.println("[Config] STREAM → tarea OK");

            // 2. FANOUT → kotxea / publikoa / k_p
            channel.exchangeDeclare(EXCHANGE_FANOUT, "fanout", true);
            channel.queueDeclare(QUEUE_KOTXEA,  true, false, false, null);
            channel.queueDeclare(QUEUE_PUBLIKO, true, false, false, null);
            channel.queueDeclare(QUEUE_KP,      true, false, false, null);
            channel.queueBind(QUEUE_KOTXEA,  EXCHANGE_FANOUT, "");
            channel.queueBind(QUEUE_PUBLIKO, EXCHANGE_FANOUT, "");
            channel.queueBind(QUEUE_KP,      EXCHANGE_FANOUT, "");
            System.out.println("[Config] FANOUT → kotxea/publikoa/k_p OK");

            // 3. EMAITZA (direct) → Q:emaitza
            channel.exchangeDeclare(EXCHANGE_EMAITZA, "direct", true);
            channel.queueDeclare(QUEUE_EMAITZA, true, false, false, null);
            channel.queueBind(QUEUE_EMAITZA, EXCHANGE_EMAITZA, QUEUE_EMAITZA);
            System.out.println("[Config] EMAITZA → emaitza OK");

            // 4. LAINOA (fanout) — cola anónima, Lainoa se suscribe al arrrancar
            channel.exchangeDeclare(EXCHANGE_LAINOA, "fanout", true);
            System.out.println("[Config] LAINOA (fanout) OK");

            System.out.println("[Config] ✔ Configuración completada.");

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new KafkaStreamConfig().configurar();
    }
}
