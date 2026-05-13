package garraioa;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * ResultWorker - Thread Pool final (Thread pool 1/2/3 de la capa de resultados).
 *
 * Flujo:
 *   Q: emaitza → agrega garraioa mota → publica a LAINOA (nube)
 *
 * Cada mensaje recibido lleva la clasificación de los tres workers (C, P, KP).
 * El ResultWorker aplica un árbol de decisión final para determinar el
 * "garraioa mota" definitivo y lo publica al exchange de la nube.
 *
 * Árbol de decisión:
 *   KOTXEA  → garraioa = "Coche"
 *   BUS     → garraioa = "Bus"
 *   TREN    → garraioa = "Tren"
 *   OINEZ   → garraioa = "A pie"
 *   KORRIKA → garraioa = "Corriendo"
 *   TXIRRINA → garraioa = "Bicicleta"
 *   PATINETE → garraioa = "Patinete"
 *   EZ_*    → garraioa = "Desconocido"
 *
 * Formato mensaje entrada:  "sensorId CLASIFICACION lat lon"
 * Formato mensaje salida:   "sensorId GarraioaMota lat lon timestamp"
 */
public class ResultWorker {

    static final String EXCHANGE_LAINOA = "lainoa";   // Exchange de la nube (salida final)

    ConnectionFactory factory;
    String workerId;

    public ResultWorker(String workerId) {
        this.workerId = workerId;
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
    }

    public void suscribir() {
        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_EMAITZA, "direct");
            channel.exchangeDeclare(EXCHANGE_LAINOA, "fanout"); // Nube: fanout para múltiples consumidores

            channel.queueDeclare(KafkaStreamConfig.QUEUE_EMAITZA, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_EMAITZA,
                              KafkaStreamConfig.EXCHANGE_EMAITZA,
                              KafkaStreamConfig.QUEUE_EMAITZA);

            channel.basicQos(1);
            channel.basicConsume(KafkaStreamConfig.QUEUE_EMAITZA, false, new MiConsumer(channel));

            System.out.println("[ResultWorker-" + workerId + "] Esperando resultados en Q:emaitza...");

            synchronized (this) {
                try { wait(); } catch (InterruptedException e) { e.printStackTrace(); }
            }
            channel.close();

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public synchronized void parar() { notify(); }

    /**
     * Mapea la etiqueta de clasificación al nombre final de garraioa mota.
     */
    static String resolverGarraioaMota(String clasificacion) {
        switch (clasificacion) {
            case "KOTXEA":   return "Coche";
            case "BUS":      return "Bus";
            case "TREN":     return "Tren";
            case "OINEZ":    return "A pie";
            case "KORRIKA":  return "Corriendo";
            case "TXIRRINA": return "Bicicleta";
            case "PATINETE": return "Patinete";
            default:         return "Desconocido";
        }
    }

    public class MiConsumer extends DefaultConsumer {

        public MiConsumer(Channel channel) { super(channel); }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope,
                                   AMQP.BasicProperties properties, byte[] body) throws IOException {

            String mensaje = new String(body, "UTF-8");
            System.out.println("[ResultWorker-" + workerId + "] Recibido: " + mensaje);

            // Parsear: sensorId CLASIFICACION lat lon
            String[] p = mensaje.split(" ");
            if (p.length < 4) {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                return;
            }

            String sensorId      = p[0];
            String clasificacion = p[1];
            String lat           = p[2];
            String lon           = p[3];
            long   timestamp     = System.currentTimeMillis();

            String garraioaMota = resolverGarraioaMota(clasificacion);

            // Mensaje final hacia la nube
            String mensajeNube = String.format("%s %s %s %s %d",
                    sensorId, garraioaMota, lat, lon, timestamp);

            getChannel().basicPublish(EXCHANGE_LAINOA, "", null, mensajeNube.getBytes());

            System.out.println("[ResultWorker-" + workerId + "] → LAINOA: " + mensajeNube);
            getChannel().basicAck(envelope.getDeliveryTag(), false);
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("ID de este ResultWorker (1/2/3): ");
        String id = teclado.nextLine();
        System.out.println("Pulsa ENTER para parar.");

        ResultWorker worker = new ResultWorker(id);
        new Thread(() -> {
            teclado.nextLine();
            worker.parar();
        }).start();

        worker.suscribir();
        teclado.close();
    }
}
