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
 * WorkerP - Clasificador: Bus / Tren / Ez P.
 *
 * Solo publica a Q:emaitza si la clasificación es positiva (BUS o TREN).
 *
 * Formato entrada:  "userId empresaId media max min distantzia lat lon timestamp"
 * Formato salida:   "userId empresaId BUS|TREN lat lon timestamp"
 */
public class WorkerP {

    static final double BUS_VEL_MIN   = 15.0;
    static final double BUS_VEL_MAX   = 60.0;
    static final double BUS_DIST_MAX  = 20.0;
    static final double TREN_VEL_MIN  = 60.0;
    static final double TREN_VEL_MAX  = 200.0;
    static final double TREN_DIST_MAX = 30.0;

    ConnectionFactory factory;
    String workerId;

    public WorkerP(String workerId) {
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

            channel.queueDeclare(KafkaStreamConfig.QUEUE_PUBLIKO, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_PUBLIKO, KafkaStreamConfig.EXCHANGE_FANOUT, "");

            channel.queueDeclare(KafkaStreamConfig.QUEUE_EMAITZA, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_EMAITZA, KafkaStreamConfig.EXCHANGE_EMAITZA,
                              KafkaStreamConfig.QUEUE_EMAITZA);

            channel.basicQos(1);
            channel.basicConsume(KafkaStreamConfig.QUEUE_PUBLIKO, false, new MiConsumer(channel));

            System.out.println("[WorkerP-" + workerId + "] Esperando en Q:publikoa...");
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
            System.out.println("[WorkerP-" + workerId + "] Recibido: " + mensaje);

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

            String clasificacion = null;
            if (media >= BUS_VEL_MIN && media <= BUS_VEL_MAX && distantzia < BUS_DIST_MAX) {
                clasificacion = "BUS";
            } else if (media > TREN_VEL_MIN && media <= TREN_VEL_MAX && distantzia < TREN_DIST_MAX) {
                clasificacion = "TREN";
            }

            System.out.println("[WorkerP-" + workerId + "] userId=" + userId +
                    " media=" + media + " dist=" + distantzia +
                    " → " + (clasificacion != null ? clasificacion + " ✓" : "EZ_P (no publica)"));

            if (clasificacion != null) {
                String resultado = userId + " " + empresaId + " " + clasificacion + " " + lat + " " + lon + " " + timestamp;
                getChannel().basicPublish(KafkaStreamConfig.EXCHANGE_EMAITZA,
                                          KafkaStreamConfig.QUEUE_EMAITZA,
                                          null, resultado.getBytes());
                System.out.println("[WorkerP-" + workerId + "] → emaitza: " + resultado);
            }

            getChannel().basicAck(envelope.getDeliveryTag(), false);
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("ID de este WorkerP: ");
        String id = teclado.nextLine();
        System.out.println("Pulsa ENTER para parar.");
        WorkerP worker = new WorkerP(id);
        new Thread(() -> { teclado.nextLine(); worker.parar(); }).start();
        worker.suscribir();
        teclado.close();
    }
}
