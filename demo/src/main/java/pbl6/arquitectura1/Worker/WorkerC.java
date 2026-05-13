package pbl6.arquitectura1.Worker;

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
 * WorkerC - Clasificador: Kotxea / Ez kotxe.
 *
 * Solo publica a Q:emaitza si la clasificación es positiva (KOTXEA).
 * Esto evita que los 3 workers inunden la cola con resultados negativos
 * duplicados para el mismo mensaje.
 *
 * Formato entrada:  "userId empresaId media max min distantzia lat lon timestamp"
 * Formato salida:   "userId empresaId KOTXEA lat lon timestamp"
 */
public class WorkerC {

    static final double UMBRAL_VEL  = 30.0;
    static final double UMBRAL_DIST = 5.0;

    ConnectionFactory factory;
    String workerId;

    public WorkerC(String workerId) {
        this.workerId = workerId;
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
    }

    public void suscribir() {
        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_FANOUT,  "fanout", true);
            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_EMAITZA, "direct", true);

            channel.queueDeclare(KafkaStreamConfig.QUEUE_KOTXEA,  true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_KOTXEA, KafkaStreamConfig.EXCHANGE_FANOUT, "");

            channel.queueDeclare(KafkaStreamConfig.QUEUE_EMAITZA, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_EMAITZA, KafkaStreamConfig.EXCHANGE_EMAITZA,
                              KafkaStreamConfig.QUEUE_EMAITZA);

            channel.basicQos(1);
            channel.basicConsume(KafkaStreamConfig.QUEUE_KOTXEA, false, new MiConsumer(channel));

            System.out.println("[WorkerC-" + workerId + "] Esperando en Q:kotxea...");
            synchronized (this) {
                try { wait(); } catch (InterruptedException e) { e.printStackTrace(); }
            }
            channel.close();

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public synchronized void parar() { notify(); }

    public class MiConsumer extends DefaultConsumer {

        public MiConsumer(Channel channel) { super(channel); }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope,
                                   AMQP.BasicProperties properties, byte[] body) throws IOException {

            String mensaje = new String(body, "UTF-8");
            System.out.println("[WorkerC-" + workerId + "] Recibido: " + mensaje);

            // Formato: "userId empresaId media max min distantzia lat lon timestamp"
            String[] p = mensaje.split(" ");
            if (p.length < 9) {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                return;
            }

            int    userId    = Integer.parseInt(p[0]);
            int    empresaId = Integer.parseInt(p[1]);
            double media      = Double.parseDouble(p[2]);
            double distantzia = Double.parseDouble(p[5]);
            String lat        = p[6];
            String lon        = p[7];
            String timestamp  = p[8];

            boolean esKotxea = media > UMBRAL_VEL && distantzia > UMBRAL_DIST;

            System.out.println("[WorkerC-" + workerId + "] userId=" + userId +
                    " media=" + media + " dist=" + distantzia +
                    " → " + (esKotxea ? "KOTXEA ✓" : "EZ_KOTXE (no publica)"));

            // Solo publica si la clasificación es positiva
            if (esKotxea) {
                String resultado = userId + " " + empresaId + " KOTXEA " + lat + " " + lon + " " + timestamp;
                getChannel().basicPublish(KafkaStreamConfig.EXCHANGE_EMAITZA,
                                          KafkaStreamConfig.QUEUE_EMAITZA,
                                          null, resultado.getBytes());
                System.out.println("[WorkerC-" + workerId + "] → emaitza: " + resultado);
            }

            getChannel().basicAck(envelope.getDeliveryTag(), false);
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("ID de este WorkerC: ");
        String id = teclado.nextLine();
        System.out.println("Pulsa ENTER para parar.");
        WorkerC worker = new WorkerC(id);
        new Thread(() -> { teclado.nextLine(); worker.parar(); }).start();
        worker.suscribir();
        teclado.close();
    }
}
