package pacientes;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Arquitectura 2 – Publisher en lote (Laino 1 / HBase simulado)
 *
 * Recoge N mediciones de pacientes y las publica de golpe
 * en el exchange fanout "pulsaciones".  El exchange distribuye
 * cada mensaje a TODAS las colas enlazadas (entre ellas Q:CO2).
 */
public class PublisherLote {

    static final String EXCHANGE_NAME = "pulsaciones";   // fanout
    static final int    TAMANO_LOTE   = 5;               // mensajes por lote
    static final int    PAUSA_MS      = 2_000;           // espera entre lotes

    private final ConnectionFactory factory;

    public PublisherLote() {
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
    }

    /**
     * Genera lotes de pulsaciones simuladas y los publica en el exchange.
     * Cada mensaje tiene formato: "<idPaciente> <pulsaciones>"
     */
    public void publicarEnLote() {
        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            Random rnd = new Random();
            String[] pacientes = {"P1", "P2", "P3", "P4"};

            while (!Thread.currentThread().isInterrupted()) {

                // ── construir un lote ──────────────────────────────────────
                List<String> lote = new ArrayList<>();
                for (int i = 0; i < TAMANO_LOTE; i++) {
                    String id  = pacientes[rnd.nextInt(pacientes.length)];
                    int    pul = rnd.nextInt(80) + 60;   // 60–139 ppm
                    lote.add(id + " " + pul);
                }

                // ── publicar cada mensaje del lote ────────────────────────
                for (String msg : lote) {
                    channel.basicPublish(EXCHANGE_NAME, "", null, msg.getBytes());
                    System.out.println("[Publisher] enviado: " + msg);
                }
                System.out.println("[Publisher] lote de " + TAMANO_LOTE + " mensajes enviado.\n");

                Thread.sleep(PAUSA_MS);
            }

        } catch (IOException | TimeoutException | InterruptedException e) {
            System.out.println("[Publisher] parado: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        PublisherLote pub = new PublisherLote();
        System.out.println("[Publisher] Iniciando envío en lote. Ctrl+C para parar.");
        pub.publicarEnLote();
    }
}
