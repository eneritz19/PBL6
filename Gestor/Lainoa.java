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
 * Lainoa - Consumidor final en la nube (equivalente a la alarma del ejercicio).
 *
 * Recibe todos los resultados clasificados desde ResultWorker
 * y los almacena/muestra (en un caso real, los guardaría en BBDD o dashboard).
 *
 * Flujo:
 *   EXCHANGE_LAINOA (fanout) → Lainoa
 *
 * Formato mensaje recibido: "sensorId GarraioaMota lat lon timestamp"
 */
public class Lainoa {

    static final String EXCHANGE_LAINOA = "lainoa";

    ConnectionFactory factory;

    public Lainoa() {
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
    }

    public void suscribir() {
        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_LAINOA, "fanout");

            // Cola anónima exclusiva para esta instancia de Lainoa
            String cola = channel.queueDeclare().getQueue();
            channel.queueBind(cola, EXCHANGE_LAINOA, "");

            channel.basicConsume(cola, true, new MiConsumer(channel));

            System.out.println("[Lainoa] ☁ Conectada. Esperando datos de garraioa...");
            System.out.println("─────────────────────────────────────────────");

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

            // Parsear: sensorId GarraioaMota lat lon timestamp
            String[] p = mensaje.split(" ");
            if (p.length >= 5) {
                String sensorId     = p[0];
                String garraioaMota = p[1];
                String lat          = p[2];
                String lon          = p[3];
                long   ts           = Long.parseLong(p[4]);

                System.out.printf("[Lainoa] %-12s → %-12s  📍 %s, %s  🕒 %d%n",
                        sensorId, garraioaMota, lat, lon, ts);
            } else {
                System.out.println("[Lainoa] Mensaje recibido: " + mensaje);
            }
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.println("☁ [Lainoa] Servicio de nube iniciado. Pulsa ENTER para parar.");

        Lainoa lainoa = new Lainoa();
        new Thread(() -> {
            teclado.nextLine();
            lainoa.parar();
        }).start();

        lainoa.suscribir();
        teclado.close();
    }
}
