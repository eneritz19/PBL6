package pbl6.arquitectura1.Resultado;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import pbl6.arquitectura1.Publisher.KafkaStreamConfig;

/**
 * ResultWorker - Consumidor final de Q:emaitza (Thread Pool final).
 *
 * Recibe clasificaciones positivas de WorkerC/P/KP y las reenvía a Lainoa.
 *
 * Formato entrada:  "userId empresaId CLASIFICACION lat lon timestamp"
 * Formato salida:   "userId empresaId GarraioaMota lat lon timestamp"
 */
public class ResultWorker {

    static final String EXCHANGE_LAINOA = "lainoa";

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

            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_EMAITZA, "direct", true);
            channel.exchangeDeclare(EXCHANGE_LAINOA, "fanout", true); // declarado aquí también

            channel.queueDeclare(KafkaStreamConfig.QUEUE_EMAITZA, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_EMAITZA,
                              KafkaStreamConfig.EXCHANGE_EMAITZA,
                              KafkaStreamConfig.QUEUE_EMAITZA);

            channel.basicQos(1);
            channel.basicConsume(KafkaStreamConfig.QUEUE_EMAITZA, false, new MiConsumer(channel));

            System.out.println("[ResultWorker-" + workerId + "] Esperando en Q:emaitza...");
            synchronized (this) {
                try { wait(); } catch (InterruptedException e) { e.printStackTrace(); }
            }
            channel.close();

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public synchronized void parar() { notify(); }

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

            // Formato: "userId empresaId CLASIFICACION lat lon timestamp"
            String[] p = mensaje.split(" ");
            if (p.length < 6) {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                return;
            }

            String userId        = p[0];
            String empresaId     = p[1];
            String clasificacion = p[2];
            String lat           = p[3];
            String lon           = p[4];
            String timestamp     = p[5];

            String garraioaMota = resolverGarraioaMota(clasificacion);

            String mensajeNube = String.format("%s %s %s %s %s %s",
                    userId, empresaId, garraioaMota, lat, lon, timestamp);

            getChannel().basicPublish(EXCHANGE_LAINOA, "", null, mensajeNube.getBytes());
            System.out.println("[ResultWorker-" + workerId + "] → Lainoa: " + mensajeNube);

            getChannel().basicAck(envelope.getDeliveryTag(), false);
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("ID de este ResultWorker (1/2/3): ");
        String id = teclado.nextLine();
        System.out.println("Pulsa ENTER para parar.");
        ResultWorker worker = new ResultWorker(id);
        new Thread(() -> { teclado.nextLine(); worker.parar(); }).start();
        worker.suscribir();
        teclado.close();
    }
}
