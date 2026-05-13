package garraioa;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * KafkaStreamConfig - Declara todos los exchanges y colas de la arquitectura.
 *
 * Exchanges:
 *   - stream_garraioa  (direct)  → Q: tarea  → TaskWorkers (Thread Pool 1/2/3)
 *   - fanout_garraioa  (fanout)  → Q: kotxea, Q: publikoa, Q: k_p
 *   - emaitza_garraioa (direct)  → Q: emaitza → ResultWorkers (Thread Pool final)
 *
 * Uso: ejecutar una vez al arrancar el sistema para asegurar que
 * todos los exchanges y colas existen antes de lanzar workers.
 */
public class KafkaStreamConfig {

    // ── Exchanges ────────────────────────────────────────────────
    public static final String EXCHANGE_STREAM  = "stream_garraioa";   // direct  (entrada)
    public static final String EXCHANGE_FANOUT  = "fanout_garraioa";   // fanout  (clasificación)
    public static final String EXCHANGE_EMAITZA = "emaitza_garraioa";  // direct  (resultados)

    // ── Routing keys / nombres de cola ───────────────────────────
    public static final String QUEUE_TAREA   = "tarea";    // TaskWorkers
    public static final String QUEUE_KOTXEA  = "kotxea";   // WorkerC
    public static final String QUEUE_PUBLIKO = "publikoa"; // WorkerP
    public static final String QUEUE_KP      = "k_p";      // WorkerKP
    public static final String QUEUE_EMAITZA = "emaitza";  // ResultWorkers

    private ConnectionFactory factory;

    public KafkaStreamConfig() {
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
    }

    /**
     * Declara todos los exchanges y colas.
     * Idempotente: se puede llamar varias veces sin error.
     */
    public void configurar() {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // 1. Exchange STREAM (direct) + cola tarea
            channel.exchangeDeclare(EXCHANGE_STREAM, "direct", true);
            channel.queueDeclare(QUEUE_TAREA, true, false, false, null);
            channel.queueBind(QUEUE_TAREA, EXCHANGE_STREAM, QUEUE_TAREA);
            System.out.println("[Config] Exchange '" + EXCHANGE_STREAM + "' (direct) → cola '" + QUEUE_TAREA + "' OK");

            // 2. Exchange FANOUT + colas de clasificación
            channel.exchangeDeclare(EXCHANGE_FANOUT, "fanout", true);

            channel.queueDeclare(QUEUE_KOTXEA,  true, false, false, null);
            channel.queueDeclare(QUEUE_PUBLIKO, true, false, false, null);
            channel.queueDeclare(QUEUE_KP,      true, false, false, null);

            // fanout: routing key ignorada, se enlaza con ""
            channel.queueBind(QUEUE_KOTXEA,  EXCHANGE_FANOUT, "");
            channel.queueBind(QUEUE_PUBLIKO, EXCHANGE_FANOUT, "");
            channel.queueBind(QUEUE_KP,      EXCHANGE_FANOUT, "");
            System.out.println("[Config] Exchange '" + EXCHANGE_FANOUT + "' (fanout) → colas kotxea/publikoa/k_p OK");

            // 3. Exchange EMAITZA (direct) + cola emaitza
            channel.exchangeDeclare(EXCHANGE_EMAITZA, "direct", true);
            channel.queueDeclare(QUEUE_EMAITZA, true, false, false, null);
            channel.queueBind(QUEUE_EMAITZA, EXCHANGE_EMAITZA, QUEUE_EMAITZA);
            System.out.println("[Config] Exchange '" + EXCHANGE_EMAITZA + "' (direct) → cola '" + QUEUE_EMAITZA + "' OK");

            System.out.println("[Config] ✔ Configuración completada.");

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new KafkaStreamConfig().configurar();
    }
}
