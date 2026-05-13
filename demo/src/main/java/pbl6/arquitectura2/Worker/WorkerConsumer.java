package pacientes;

import com.rabbitmq.client.Channel;

import java.io.IOException;

/**
 * Arquitectura 2 – Worker individual del Thread Pool
 *
 * Cada instancia de esta clase representa uno de los hilos T1…TN
 * del pool.  Recibe el cuerpo de UN mensaje ya extraído de la cola
 * Q:CO2 y ejecuta la lógica de negocio:
 *   1. Parsea "idPaciente pulsaciones"
 *   2. Actualiza la ventana deslizante en GestorPacientes (thread-safe)
 *   3. Si la media supera el límite, publica una alarma en el exchange "alarma" (direct)
 */
public class WorkerConsumer implements Runnable {

    static final String EXCHANGE_ALARMA = "alarma";
    static final int    LIMITE          = 100;

    private final Channel         channel;
    private final GestorPacientes gestor;
    private final String          mensaje;
    private final long            deliveryTag;
    private final boolean         autoAck;

    /**
     * @param channel     canal RabbitMQ compartido (thread-safe para basicPublish/basicAck)
     * @param gestor      estado compartido de medias por paciente
     * @param mensaje     cuerpo del mensaje en texto plano
     * @param deliveryTag etiqueta para el ack manual
     * @param autoAck     true → no se hace ack explícito (modo sencillo)
     */
    public WorkerConsumer(Channel channel, GestorPacientes gestor,
                          String mensaje, long deliveryTag, boolean autoAck) {
        this.channel     = channel;
        this.gestor      = gestor;
        this.mensaje     = mensaje;
        this.deliveryTag = deliveryTag;
        this.autoAck     = autoAck;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        System.out.println("[" + threadName + "] procesando: " + mensaje);

        try {
            // ── parsear mensaje ──────────────────────────────────────────
            String[] partes      = mensaje.split(" ");
            String   id          = partes[0];
            int      pulsaciones = Integer.parseInt(partes[1]);

            // ── calcular media (sincronizado dentro de GestorPacientes) ──
            double media = gestor.getMedia(id, pulsaciones);

            // ── emitir alarma si procede ─────────────────────────────────
            if (media > LIMITE) {
                String alarma = "ALARMA paciente " + id + " – media=" + String.format("%.1f", media);
                channel.basicPublish(EXCHANGE_ALARMA, id, null, alarma.getBytes());
                System.out.println("[" + threadName + "] ALARMA publicada → " + alarma);
            }

            // ── ack manual si corresponde ────────────────────────────────
            if (!autoAck) {
                channel.basicAck(deliveryTag, false);
            }

        } catch (IOException e) {
            System.err.println("[" + threadName + "] error al procesar: " + e.getMessage());
            // nack sin requeue en caso de mensaje malformado
            try {
                if (!autoAck) channel.basicNack(deliveryTag, false, false);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
