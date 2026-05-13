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
 * TaskWorker - Consumidor de Q:tarea (Thread Pool 1/2/3).
 *
 * Flujo:
 *   Q: tarea → calcula media/max/min/distantzia → publica al EXCHANGE_FANOUT
 *
 * Formato mensaje entrada:  "userId empresaId lat lon velocidad"
 * Formato mensaje salida:   "userId empresaId media max min distantzia lat lon timestamp"
 */
public class TaskWorker {

    ConnectionFactory factory;
    GestorDatos gestorDatos;
    String workerId;

    public TaskWorker(String workerId) {
        this.workerId   = workerId;
        factory         = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
        gestorDatos     = new GestorDatos();
    }

    public void suscribir() {
        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_STREAM, "direct", true);
            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_FANOUT, "fanout", true);
            channel.queueDeclare(KafkaStreamConfig.QUEUE_TAREA, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_TAREA,
                              KafkaStreamConfig.EXCHANGE_STREAM,
                              KafkaStreamConfig.QUEUE_TAREA);

            channel.basicQos(1); // fair dispatch: un mensaje a la vez
            channel.basicConsume(KafkaStreamConfig.QUEUE_TAREA, false, new MiConsumer(channel));

            System.out.println("[TaskWorker-" + workerId + "] Esperando tareas en Q:tarea...");

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
            System.out.println("[TaskWorker-" + workerId + "] Recibido: " + mensaje);

            // Formato: "userId empresaId lat lon velocidad"
            String[] p = mensaje.split(" ");
            if (p.length < 5) {
                System.err.println("[TaskWorker-" + workerId + "] Mensaje malformado, ignorado: " + mensaje);
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                return;
            }

            int    userId    = Integer.parseInt(p[0]);
            int    empresaId = Integer.parseInt(p[1]);
            double lat       = Double.parseDouble(p[2]);
            double lon       = Double.parseDouble(p[3]);
            double velocidad = Double.parseDouble(p[4]);
            long   timestamp = System.currentTimeMillis();

            GestorDatos.Estadisticas est =
                    gestorDatos.calcular(userId, empresaId, lat, lon, timestamp, velocidad);

            if (est.completo) {
                // Formato salida: "userId empresaId media max min distantzia lat lon timestamp"
                String resultado = String.format("%d %d %.2f %.2f %.2f %.2f %.6f %.6f %d",
                        est.userId, est.empresaId,
                        est.media, est.max, est.min, est.distantzia,
                        est.latitud, est.longitud, est.timestamp);

                getChannel().basicPublish(KafkaStreamConfig.EXCHANGE_FANOUT, "", null, resultado.getBytes());
                System.out.println("[TaskWorker-" + workerId + "] → fanout: " + resultado);
            } else {
                System.out.println("[TaskWorker-" + workerId + "] Acumulando datos para userId=" + userId +
                        " (" + gestorDatos.datosVelocidad.get(userId).size() + "/" + GestorDatos.NUM_DATOS + ")");
            }

            getChannel().basicAck(envelope.getDeliveryTag(), false);
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("ID de este TaskWorker (1/2/3): ");
        String id = teclado.nextLine();
        System.out.println("Pulsa ENTER para parar.");

        TaskWorker worker = new TaskWorker(id);
        new Thread(() -> { teclado.nextLine(); worker.parar(); }).start();
        worker.suscribir();
        teclado.close();
    }
}
