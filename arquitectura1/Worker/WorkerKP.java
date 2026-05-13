package garraioa;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP;

/**
 * WorkerKP - Clasificador: Oinez / Korrika / Txirrina / Patinete / Ez KP.
 *
 * Solo publica a Q:emaitza si la clasificación es positiva (no EZ_KP).
 *
 * Formato entrada:  "userId empresaId media max min distantzia lat lon timestamp"
 * Formato salida:   "userId empresaId OINEZ|KORRIKA|TXIRRINA|PATINETE lat lon timestamp"
 */
public class WorkerKP {

    ConnectionFactory factory;
    String workerId;

    public WorkerKP(String workerId) {
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

            channel.queueDeclare(KafkaStreamConfig.QUEUE_KP, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_KP, KafkaStreamConfig.EXCHANGE_FANOUT, "");

            channel.queueDeclare(KafkaStreamConfig.QUEUE_EMAITZA, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_EMAITZA, KafkaStreamConfig.EXCHANGE_EMAITZA,
                              KafkaStreamConfig.QUEUE_EMAITZA);

            channel.basicQos(1);
            channel.basicConsume(KafkaStreamConfig.QUEUE_KP, false, new MiConsumer(channel));

            System.out.println("[WorkerKP-" + workerId + "] Esperando en Q:k_p...");
            synchronized (this) {
                try { wait(); } catch (InterruptedException e) { e.printStackTrace(); }
            }
            channel.close();

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public synchronized void parar() { notify(); }

    static String clasificar(double media, double distantzia) {
        if (media < 6.0)       return "OINEZ";
        if (media < 15.0)      return "KORRIKA";
        if (media < 30.0)      return distantzia < 5.0 ? "TXIRRINA" : "PATINETE";
        return null; // EZ_KP → no publicar
    }

    public class MiConsumer extends DefaultConsumer {

        public MiConsumer(Channel channel) { super(channel); }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope,
                                   AMQP.BasicProperties properties, byte[] body) throws IOException {

            String mensaje = new String(body, "UTF-8");
            System.out.println("[WorkerKP-" + workerId + "] Recibido: " + mensaje);

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

            String clasificacion = clasificar(media, distantzia);

            System.out.println("[WorkerKP-" + workerId + "] userId=" + userId +
                    " media=" + media + " dist=" + distantzia +
                    " → " + (clasificacion != null ? clasificacion + " ✓" : "EZ_KP (no publica)"));

            if (clasificacion != null) {
                String resultado = userId + " " + empresaId + " " + clasificacion + " " + lat + " " + lon + " " + timestamp;
                getChannel().basicPublish(KafkaStreamConfig.EXCHANGE_EMAITZA,
                                          KafkaStreamConfig.QUEUE_EMAITZA,
                                          null, resultado.getBytes());
                System.out.println("[WorkerKP-" + workerId + "] → emaitza: " + resultado);
            }

            getChannel().basicAck(envelope.getDeliveryTag(), false);
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("ID de este WorkerKP: ");
        String id = teclado.nextLine();
        System.out.println("Pulsa ENTER para parar.");
        WorkerKP worker = new WorkerKP(id);
        new Thread(() -> { teclado.nextLine(); worker.parar(); }).start();
        worker.suscribir();
        teclado.close();
    }
}
