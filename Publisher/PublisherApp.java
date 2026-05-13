package garraioa;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * PublisherApp - Publica datos de sensor cada minuto al exchange de Kafka/RabbitMQ.
 * Cada mensaje contiene: id_sensor latitud longitud velocidad
 * Arkitektura 1: Nodo inicial del flujo
 */
public class PublisherApp {

    final static String EXCHANGE_STREAM = "stream_garraioa";
    final static int INTERVALO_MS = 60000; // 1 minuto

    ConnectionFactory factory;
    Channel channel;
    String sensorId;
    HiloPublicador hiloPublicador;

    public PublisherApp(String sensorId) {
        this.sensorId = sensorId;
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
    }

    public void iniciar() {
        try (Connection connection = factory.newConnection()) {
            channel = connection.createChannel();

            // Exchange tipo direct hacia los TaskWorkers (Thread Pool 1/2/3)
            channel.exchangeDeclare(EXCHANGE_STREAM, "direct");

            hiloPublicador = new HiloPublicador();
            hiloPublicador.start();

            synchronized (this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            channel.close();

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public void parar() {
        if (hiloPublicador != null) hiloPublicador.interrupt();
        synchronized (this) {
            notify();
        }
    }

    /**
     * Hilo que genera y publica datos del sensor periódicamente.
     * Simula: id_sensor latitud longitud velocidad(km/h)
     */
    public class HiloPublicador extends Thread {
        @Override
        public void run() {
            Random rnd = new Random();
            try {
                while (!this.isInterrupted()) {
                    double lat = 43.0 + rnd.nextDouble() * 0.1;
                    double lon = -2.5 + rnd.nextDouble() * 0.1;
                    double velocidad = rnd.nextDouble() * 120; // 0-120 km/h

                    String mensaje = sensorId + " " + lat + " " + lon + " " + velocidad;

                    if (!this.isInterrupted()) {
                        // Publica con routing key = sensorId para distribución direct
                        channel.basicPublish(EXCHANGE_STREAM, "tarea", null, mensaje.getBytes());
                        System.out.println("[Publisher] Enviado: " + mensaje);
                        Thread.sleep(INTERVALO_MS);
                    }
                }
            } catch (IOException | InterruptedException e) {
                System.out.println("[Publisher] Hilo interrumpido.");
            }
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("ID del sensor: ");
        String id = teclado.nextLine();
        System.out.println("Publicando datos cada minuto. Pulsa ENTER para parar.");

        PublisherApp publisher = new PublisherApp(id);

        new Thread(() -> {
            teclado.nextLine();
            publisher.parar();
        }).start();

        publisher.iniciar();
        teclado.close();
    }
}
