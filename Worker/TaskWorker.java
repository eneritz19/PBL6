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
 * TaskWorker - Consumidor de la cola "tarea" (Thread Pool 1, 2 o 3).
 *
 * Flujo:
 *   Q: tarea → calcula media/max/min/distantzia → publica al EXCHANGE_FANOUT
 *
 * Varias instancias de este worker compiten por los mensajes de Q: tarea
 * (RabbitMQ round-robin), simulando el Thread Pool del diagrama.
 *
 * Formato mensaje entrada:  "sensorId lat lon velocidad"
 * Formato mensaje salida:   "sensorId media max min distantzia lat lon"
 */
public class TaskWorker {

    ConnectionFactory factory;
    GestorDatos gestorDatos;
    String workerId;

    public TaskWorker(String workerId) {
        this.workerId = workerId;
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
        gestorDatos = new GestorDatos();
    }

    public void suscribir() {
        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            // Asegurar que los exchanges/colas existen
            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_STREAM,  "direct");
            channel.exchangeDeclare(KafkaStreamConfig.EXCHANGE_FANOUT,  "fanout");
            channel.queueDeclare(KafkaStreamConfig.QUEUE_TAREA, true, false, false, null);
            channel.queueBind(KafkaStreamConfig.QUEUE_TAREA,
                              KafkaStreamConfig.EXCHANGE_STREAM,
                              KafkaStreamConfig.QUEUE_TAREA);

            // prefetchCount=1: el worker sólo recibe 1 mensaje a la vez (fair dispatch)
            channel.basicQos(1);

            MiConsumer consumer = new MiConsumer(channel);
            channel.basicConsume(KafkaStreamConfig.QUEUE_TAREA, false, consumer);

            System.out.println("[TaskWorker-" + workerId + "] Esperando tareas...");

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

        public MiConsumer(Channel channel) {
            super(channel);
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope,
                                   AMQP.BasicProperties properties, byte[] body) throws IOException {

            String mensaje = new String(body, "UTF-8");
            System.out.println("[TaskWorker-" + workerId + "] Recibido: " + mensaje);

            // Parsear: sensorId lat lon velocidad
            String[] partes = mensaje.split(" ");
            if (partes.length < 4) {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                return;
            }

            String sensorId  = partes[0];
            String lat       = partes[1];
            String lon       = partes[2];
            double velocidad = Double.parseDouble(partes[3]);

            // Calcular estadísticas con ventana deslizante
            GestorDatos.Estadisticas est = gestorDatos.calcular(sensorId, velocidad);

            if (est.completo) {
                // Formato salida: "sensorId media max min distantzia lat lon"
                String resultado = String.format("%s %.2f %.2f %.2f %.2f %s %s",
                        sensorId, est.media, est.max, est.min, est.distantzia, lat, lon);

                // Publicar al exchange fanout (va a WorkerC, WorkerP, WorkerKP)
                getChannel().basicPublish(KafkaStreamConfig.EXCHANGE_FANOUT, "", null, resultado.getBytes());
                System.out.println("[TaskWorker-" + workerId + "] Publicado al fanout: " + resultado);
            } else {
                System.out.println("[TaskWorker-" + workerId + "] Acumulando datos para " + sensorId);
            }

            // Confirmar mensaje procesado
            getChannel().basicAck(envelope.getDeliveryTag(), false);
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("ID de este TaskWorker (1/2/3): ");
        String id = teclado.nextLine();
        System.out.println("Pulsa ENTER para parar.");

        TaskWorker worker = new TaskWorker(id);
        new Thread(() -> {
            teclado.nextLine();
            worker.parar();
        }).start();

        worker.suscribir();
        teclado.close();
    }
}
