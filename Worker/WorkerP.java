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
 * WorkerP - Clasificador de transporte público: Bus / Tren / Ez P.
 *
 * Flujo:
 *   Q: publikoa ← EXCHANGE_FANOUT → clasifica → publica a Q: emaitza
 *
 * Criterio de clasificación (velocidad media):
 *   - media entre 15 y 60 km/h  Y  distantzia < 20  →  BUS
 *     (velocidad moderada, paradas frecuentes → poca variación)
 *   - media entre 60 y 200 km/h  Y  distantzia < 30  →  TREN
 *     (velocidad alta, recorrido regular → variación baja)
 *   - en caso contrario                               →  EZ_P
 *
 * Formato mensaje entrada:  "sensorId media max min distantzia lat lon"
 * Formato mensaje salida:   "sensorId BUS|TREN|EZ_P lat lon"
 */
public class WorkerP {

    // Umbrales para Bus
    static final double BUS_VEL_MIN  = 15.0;
    static final double BUS_VEL_MAX  = 60.0;
    static final double BUS_DIST_MAX = 20.0;

    // Umbrales para Tren
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

            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_FANOUT,  "fanout");
            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_EMAITZA, "direct");

            channel.queueDeclare(KafkaStreamConfig.QUEUE_PUBLIKO, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_PUBLIKO, KafkaStreamConfig.EXCHANGE_FANOUT, "");

            channel.queueDeclare(KafkaStreamConfig.QUEUE_EMAITZA, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_EMAITZA, KafkaStreamConfig.EXCHANGE_EMAITZA,
                              KafkaStreamConfig.QUEUE_EMAITZA);

            channel.basicQos(1);
            channel.basicConsume(KafkaStreamConfig.QUEUE_PUBLIKO, false, new MiConsumer(channel));

            System.out.println("[WorkerP-" + workerId + "] Esperando mensajes en Q:publikoa...");

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
            if (p.length < 7) {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                return;
            }

            String sensorId   = p[0];
            double media      = Double.parseDouble(p[1]);
            double distantzia = Double.parseDouble(p[4]);
            String lat        = p[5];
            String lon        = p[6];

            String clasificacion;
            if (media >= BUS_VEL_MIN && media <= BUS_VEL_MAX && distantzia < BUS_DIST_MAX) {
                clasificacion = "BUS";
            } else if (media > TREN_VEL_MIN && media <= TREN_VEL_MAX && distantzia < TREN_DIST_MAX) {
                clasificacion = "TREN";
            } else {
                clasificacion = "EZ_P";
            }

            String resultado = sensorId + " " + clasificacion + " " + lat + " " + lon;

            getChannel().basicPublish(KafkaStreamConfig.EXCHANGE_EMAITZA,
                                      KafkaStreamConfig.QUEUE_EMAITZA,
                                      null, resultado.getBytes());

            System.out.println("[WorkerP-" + workerId + "] Clasificado → " + resultado);
            getChannel().basicAck(envelope.getDeliveryTag(), false);
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("ID de este WorkerP: ");
        String id = teclado.nextLine();
        System.out.println("Pulsa ENTER para parar.");

        WorkerP worker = new WorkerP(id);
        new Thread(() -> {
            teclado.nextLine();
            worker.parar();
        }).start();

        worker.suscribir();
        teclado.close();
    }
}
