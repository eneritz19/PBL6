package pacientes;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Arquitectura 2 – Monitor con Thread Pool (cola compartida Q:CO2)
 *
 * Flujo:
 *   Exchange fanout "pulsaciones"
 *       └─► Cola Q:CO2  (una sola cola, varios consumers comparten prefetch)
 *               └─► ExecutorService (N hilos = núcleos disponibles)
 *                       └─► WorkerConsumer (T1…TN)
 *                               └─► Exchange direct "alarma"
 *
 * basicQos(1) garantiza que RabbitMQ sólo entrega 1 mensaje a cada consumer
 * hasta que éste lo haya confirmado (ack manual), distribuyendo la carga
 * de forma equitativa entre los workers del pool.
 */
public class MonitorPool {

    static final String EXCHANGE_PULSACIONES = "pulsaciones";
    static final String EXCHANGE_ALARMA      = "alarma";
    static final String NOMBRE_COLA          = "CO2";   // cola nombrada y duradera
    static final int    N_THREADS            = Runtime.getRuntime().availableProcessors();

    private final ConnectionFactory factory;
    private final GestorPacientes   gestor;
    private final ExecutorService   pool;

    public MonitorPool() {
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");

        gestor = new GestorPacientes();
        pool   = Executors.newFixedThreadPool(N_THREADS);

        System.out.println("[MonitorPool] thread pool con " + N_THREADS + " hilos (T1…T" + N_THREADS + ")");
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Suscripción principal
    // ──────────────────────────────────────────────────────────────────────────
    public void suscribir() {
        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            // Declarar exchanges
            channel.exchangeDeclare(EXCHANGE_PULSACIONES, "fanout");
            channel.exchangeDeclare(EXCHANGE_ALARMA,      "direct");

            // Declarar cola nombrada y enlazarla al fanout
            channel.queueDeclare(NOMBRE_COLA, false, false, false, null);
            channel.queueBind(NOMBRE_COLA, EXCHANGE_PULSACIONES, "");

            // Un mensaje por consumer (prefetch) → reparto equitativo
            channel.basicQos(1);

            // Consumer que delega al pool
            boolean autoAck = false;  // ack manual para no perder mensajes
            channel.basicConsume(NOMBRE_COLA, autoAck, new PoolDispatcher(channel));

            System.out.println("[MonitorPool] esperando mensajes en cola '" + NOMBRE_COLA + "'…");

            // Esperar señal de parada
            synchronized (this) {
                this.wait();
            }

            // Apagado ordenado del pool
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);

        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized void parar() {
        this.notify();
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Consumer interno: encola cada entrega en el pool
    // ──────────────────────────────────────────────────────────────────────────
    private class PoolDispatcher extends DefaultConsumer {

        public PoolDispatcher(Channel channel) {
            super(channel);
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope,
                                   AMQP.BasicProperties properties, byte[] body)
                throws IOException {

            String mensaje     = new String(body, "UTF-8");
            long   deliveryTag = envelope.getDeliveryTag();

            // Enviar al pool → uno de los N workers lo ejecutará
            pool.submit(new WorkerConsumer(
                    getChannel(), gestor, mensaje, deliveryTag, false));
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Main
    // ──────────────────────────────────────────────────────────────────────────
    public static void main(String[] args) {
        Scanner      teclado = new Scanner(System.in);
        MonitorPool  monitor = new MonitorPool();

        System.out.println("Pulsa ENTER para detener el monitor.");
        new Thread(() -> {
            teclado.nextLine();
            monitor.parar();
        }).start();

        monitor.suscribir();
        teclado.close();
    }
}
