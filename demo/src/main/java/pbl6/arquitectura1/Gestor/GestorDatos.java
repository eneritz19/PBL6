package pbl6.arquitectura1.Gestor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GestorDatos - Ventana deslizante de velocidades por userId.
 *
 * NOTA simulacro: cada TaskWorker tiene su propio GestorDatos.
 * Si el round-robin reparte mensajes del mismo userId entre varios
 * workers, cada uno acumula su propia ventana parcial.
 * En producción habría que centralizar esto (Redis, BD compartida, etc.)
 */
public class GestorDatos {

    public static final int NUM_DATOS = 5;

    // Clave: userId
    public Map<Integer, List<Double>> datosVelocidad;

    public GestorDatos() {
        datosVelocidad = new HashMap<>();
    }

    public static class Estadisticas {
        public final int    userId;
        public final int    empresaId;
        public final double latitud;
        public final double longitud;
        public final long   timestamp;
        public final double media;
        public final double max;
        public final double min;
        public final double distantzia;
        public final boolean completo;

        public Estadisticas(int userId, int empresaId, double latitud, double longitud,
                            long timestamp, double media, double max, double min,
                            double distantzia, boolean completo) {
            this.userId     = userId;
            this.empresaId  = empresaId;
            this.latitud    = latitud;
            this.longitud   = longitud;
            this.timestamp  = timestamp;
            this.media      = media;
            this.max        = max;
            this.min        = min;
            this.distantzia = distantzia;
            this.completo   = completo;
        }

        @Override
        public String toString() {
            if (!completo) return "userId=" + userId + " [acumulando datos...]";
            return String.format("userId=%d empresaId=%d lat=%.6f lon=%.6f ts=%d " +
                                 "media=%.2f max=%.2f min=%.2f dist=%.2f",
                    userId, empresaId, latitud, longitud, timestamp,
                    media, max, min, distantzia);
        }
    }

    /**
     * Acumula la velocidad del userId y calcula estadísticas cuando
     * la ventana deslizante está llena (NUM_DATOS lecturas).
     */
    public synchronized Estadisticas calcular(int userId, int empresaId,
                                               double latitud, double longitud,
                                               long timestamp, double velocidad) {
        List<Double> datos = datosVelocidad.computeIfAbsent(userId, k -> new ArrayList<>());

        if (datos.size() >= NUM_DATOS) {
            datos.remove(0); // ventana deslizante: descarta el más antiguo
        }
        datos.add(velocidad);

        if (datos.size() == NUM_DATOS) {
            double media = datos.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
            double max   = datos.stream().mapToDouble(Double::doubleValue).max().getAsDouble();
            double min   = datos.stream().mapToDouble(Double::doubleValue).min().getAsDouble();
            double dist  = max - min;
            return new Estadisticas(userId, empresaId, latitud, longitud,
                                    timestamp, media, max, min, dist, true);
        }

        return new Estadisticas(userId, empresaId, latitud, longitud,
                                timestamp, 0, 0, 0, 0, false);
    }
}
