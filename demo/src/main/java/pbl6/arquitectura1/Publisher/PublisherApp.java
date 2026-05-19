package pbl6.arquitectura1.Publisher;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * PublisherApp - Simula sensores publicando datos cada INTERVALO_MS ms.
 *
 * Formato mensaje: "userId empresaId lat lon velocidad"
 *
 * userId → identificador del usuario/sensor (int)
 * empresaId → identificador de empresa (int, fijo por instancia)
 * lat/lon → coordenadas simuladas (zona Mondragón)
 * velocidad → km/h aleatorio entre 0 y 120
 */
public class PublisherApp {

    static final String EXCHANGE_STREAM = "stream_garraioa";
    static final int INTERVALO_MS = 2000; // 2s para el simulacro (en producción: 60000)

    ConnectionFactory factory;
    Channel channel;
    int userId;
    int empresaId;
    HiloPublicador hiloPublicador;

    public PublisherApp(int userId, int empresaId) {
        this.userId = userId;
        this.empresaId = empresaId;
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
    }

    public void iniciar() {
        try (Connection connection = factory.newConnection()) {
            channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_STREAM, "direct", true);

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
        if (hiloPublicador != null)
            hiloPublicador.interrupt();
        synchronized (this) {
            notify();
        }
    }

    public class HiloPublicador extends Thread {
        @Override
        public void run() {
            Random rnd = new Random();
            try {
                while (!isInterrupted()) {
                    // Zona Mondragón aprox.
                    double lat = 43.060 + rnd.nextDouble() * 0.05;
                    double lon = -2.490 + rnd.nextDouble() * 0.05;
                    double velocidad = rnd.nextDouble() * 120.0; // 0-120 km/h

                    // Formato: "userId empresaId lat lon velocidad"
                    String mensaje = String.format(java.util.Locale.US, "%d %d %.6f %.6f %.2f",
                            userId, empresaId, lat, lon, velocidad);

                    channel.basicPublish(EXCHANGE_STREAM, "tarea", null, mensaje.getBytes());
                    System.out.println("[Publisher-" + userId + "] Enviado: " + mensaje);

                    Thread.sleep(INTERVALO_MS);
                }
            } catch (IOException | InterruptedException e) {
                System.out.println("[Publisher-" + userId + "] Hilo interrumpido.");
            }
        }
    }

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        System.out.print("userId (int): ");
        int userId = Integer.parseInt(teclado.nextLine().trim());
        System.out.print("empresaId (int): ");
        int empresaId = Integer.parseInt(teclado.nextLine().trim());
        System.out.println("Publicando cada " + INTERVALO_MS + "ms. Pulsa ENTER para parar.");

        PublisherApp publisher = new PublisherApp(userId, empresaId);
        new Thread(() -> {
            teclado.nextLine();
            publisher.parar();
        }).start();
        publisher.iniciar();
        teclado.close();
    }
}
