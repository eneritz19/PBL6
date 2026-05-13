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
 * WorkerC - Clasificador de transporte: Kotxea / Ez kotxe (Coche / No coche).
 *
 * Flujo:
 *   Q: kotxea ← EXCHANGE_FANOUT → clasifica → publica a Q: emaitza
 *
 * Criterio de clasificación por velocidad media:
 *   - media > 30 km/h  Y  distantzia > 5  →  KOTXEA  (coche: velocidad alta y variable)
 *   - en caso contrario                   →  EZ_KOTXE
 *
 * Formato mensaje entrada:  "sensorId media max min distantzia lat lon"
 * Formato mensaje salida:   "sensorId KOTXEA lat lon"  /  "sensorId EZ_KOTXE lat lon"
 */
public class WorkerC {

    static final double UMBRAL_VELOCIDAD_KOTXE = 30.0;
    static final double UMBRAL_DISTANTZIA_KOTXE = 5.0;

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

            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_FANOUT,  "fanout");
            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_EMAITZA, "direct");

            // Cola exclusiva ligada al fanout
            channel.queueDeclare(KafkaStreamConfig.QUEUE_KOTXEA, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_KOTXEA, KafkaStreamConfig.EXCHANGE_FANOUT, "");

            channel.queueDeclare(KafkaStreamConfig.QUEUE_EMAITZA, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_EMAITZA, KafkaStreamConfig.EXCHANGE_EMAITZA,
                              KafkaStreamConfig.QUEUE_EMAITZA);

            channel.basicQos(1);
            channel.basicConsume(KafkaStreamConfig.QUEUE_KOTXEA, false, new MiConsumer(channel));

            System.out.println("[WorkerC-" + workerId + "] Esperando mensajes en Q:kotxea...");

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

            // Parsear: sensorId media max min distantzia lat lon
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

            // Clasificación: velocidad alta y variación alta → coche
            String clasificacion = (media > UMBRAL_VELOCIDAD_KOTXE && distantzia > UMBRAL_DISTANTZIA_KOTXE)
                    ? "KOTXEA" : "EZ_KOTXE";

            String resultado = sensorId + " " + clasificacion + " " + lat + " " + lon;

            getChannel().basicPublish(KafkaStreamConfig.EXCHANGE_EMAITZA,
                                      KafkaStreamConfig.QUEUE_EMAITZA,
                                      null, resultado.getBytes());

            System.out.println("[WorkerC-" + workerId + "] Clasificado → " + resultado);
            getChannel().basicAck(envelope.getDeliveryTag(), false);
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("ID de este WorkerC: ");
        String id = teclado.nextLine();
        System.out.println("Pulsa ENTER para parar.");

        WorkerC worker = new WorkerC(id);
        new Thread(() -> {
            teclado.nextLine();
            worker.parar();
        }).start();

        worker.suscribir();
        teclado.close();
    }
}
