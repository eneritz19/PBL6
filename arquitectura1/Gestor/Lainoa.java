package arquitectura1.Gestor;

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
 * Lainoa - Consumidor final en la nube.
 *
 * Formato mensaje recibido: "userId empresaId GarraioaMota lat lon timestamp"
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

            channel.exchangeDeclare(EXCHANGE_LAINOA, "fanout", true);

            // Cola anónima exclusiva: cada instancia de Lainoa recibe todos los mensajes
            String cola = channel.queueDeclare().getQueue();
            channel.queueBind(cola, EXCHANGE_LAINOA, "");

            channel.basicConsume(cola, true, new MiConsumer(channel));

            System.out.println("[Lainoa] Conectada. Esperando resultados...");
            System.out.println("──────────────────────────────────────────────────────");

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

            // Formato: "userId empresaId GarraioaMota lat lon timestamp"
            String[] p = mensaje.split(" ");
            if (p.length >= 6) {
                System.out.printf("[Lainoa] userId=%-4s empresa=%-4s %-12s  lat=%s lon=%s  ts=%s%n",
                        p[0], p[1], p[2], p[3], p[4], p[5]);
            } else {
                System.out.println("[Lainoa] " + mensaje);
            }
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.println("[Lainoa] Servicio iniciado. Pulsa ENTER para parar.");
        Lainoa lainoa = new Lainoa();
        new Thread(() -> { teclado.nextLine(); lainoa.parar(); }).start();
        lainoa.suscribir();
        teclado.close();
    }
}
