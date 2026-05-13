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
 * WorkerKP - Clasificador de movilidad activa/lenta:
 *            Oinez / Korrika / Txirrina / Patinete / Ez K/P
 *
 * Flujo:
 *   Q: k_p ← EXCHANGE_FANOUT → clasifica → publica a Q: emaitza
 *
 * Criterio de clasificación (velocidad media, km/h):
 *   - media < 6              →  OINEZ      (a pie)
 *   - media entre 6 y 15     →  KORRIKA    (corriendo)
 *   - media entre 15 y 25    →  PATINETE   (patinete/monopatín)
 *   - media entre 10 y 30    →  TXIRRINA   (bicicleta)
 *     (Txirrina tiene prioridad sobre Patinete si dist < 5)
 *   - en caso contrario      →  EZ_KP
 *
 * Formato mensaje entrada:  "sensorId media max min distantzia lat lon"
 * Formato mensaje salida:   "sensorId OINEZ|KORRIKA|TXIRRINA|PATINETE|EZ_KP lat lon"
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

            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_FANOUT,  "fanout");
            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_EMAITZA, "direct");

            channel.queueDeclare(KafkaStreamConfig.QUEUE_KP, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_KP, KafkaStreamConfig.EXCHANGE_FANOUT, "");

            channel.queueDeclare(KafkaStreamConfig.QUEUE_EMAITZA, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_EMAITZA, KafkaStreamConfig.EXCHANGE_EMAITZA,
                              KafkaStreamConfig.QUEUE_EMAITZA);

            channel.basicQos(1);
            channel.basicConsume(KafkaStreamConfig.QUEUE_KP, false, new MiConsumer(channel));

            System.out.println("[WorkerKP-" + workerId + "] Esperando mensajes en Q:k_p...");

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
     * Lógica de clasificación para movilidad activa/lenta.
     */
    static String clasificar(double media, double distantzia) {
        if (media < 6.0) {
            return "OINEZ";
        } else if (media < 15.0) {
            return "KORRIKA";
        } else if (media < 30.0) {
            // Distinguir bici (Txirrina) de patinete por variación de velocidad
            return (distantzia < 5.0) ? "TXIRRINA" : "PATINETE";
        } else {
            return "EZ_KP";
        }
    }

    public class MiConsumer extends DefaultConsumer {

        public MiConsumer(Channel channel) { super(channel); }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope,
                                   AMQP.BasicProperties properties, byte[] body) throws IOException {

            String mensaje = new String(body, "UTF-8");
            System.out.println("[WorkerKP-" + workerId + "] Recibido: " + mensaje);

            String[] p = mensaje.split(" ");
            if (p.length < 7) {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                return;
            }

            String sensorId   = p[0];
            double media      = Double.parseDouble(p[1]);
            double distantzia = Double.parseDouble(p[4]);
            String lat        = p[5];
            String lon        = p[6];

            String clasificacion = clasificar(media, distantzia);
            String resultado = sensorId + " " + clasificacion + " " + lat + " " + lon;

            getChannel().basicPublish(KafkaStreamConfig.EXCHANGE_EMAITZA,
                                      KafkaStreamConfig.QUEUE_EMAITZA,
                                      null, resultado.getBytes());

            System.out.println("[WorkerKP-" + workerId + "] Clasificado → " + resultado);
            getChannel().basicAck(envelope.getDeliveryTag(), false);
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("ID de este WorkerKP: ");
        String id = teclado.nextLine();
        System.out.println("Pulsa ENTER para parar.");

        WorkerKP worker = new WorkerKP(id);
        new Thread(() -> {
            teclado.nextLine();
            worker.parar();
        }).start();

        worker.suscribir();
        teclado.close();
    }
}
