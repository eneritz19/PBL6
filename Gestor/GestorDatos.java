package garraioa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GestorDatos - Almacena y calcula estadísticas de velocidad por sensor.
 * Calcula: media, max, min, distantzia (rango = max - min)
 * Equivalente a GestorPacientes del ejercicio de referencia.
 */
public class GestorDatos {

    final static int NUM_DATOS = 5;

    // Map<sensorId, List<velocidades>>
    Map<String, List<Double>> datosVelocidad;

    public GestorDatos() {
        datosVelocidad = new HashMap<>();
    }

    /**
     * Resultado de cálculo estadístico para un sensor.
     */
    public static class Estadisticas {
        public final String sensorId;
        public final double media;
        public final double max;
        public final double min;
        public final double distantzia; // rango = max - min
        public final boolean completo;  // true si ya hay NUM_DATOS muestras

        public Estadisticas(String sensorId, double media, double max, double min,
                            double distantzia, boolean completo) {
            this.sensorId = sensorId;
            this.media = media;
            this.max = max;
            this.min = min;
            this.distantzia = distantzia;
            this.completo = completo;
        }

        @Override
        public String toString() {
            if (!completo) return sensorId + " [acumulando datos...]";
            return String.format("%s media=%.2f max=%.2f min=%.2f dist=%.2f",
                    sensorId, media, max, min, distantzia);
        }
    }

    /**
     * Agrega una nueva medición y calcula estadísticas cuando hay NUM_DATOS muestras.
     */
    public synchronized Estadisticas calcular(String sensorId, double velocidad) {
        List<Double> datos = datosVelocidad.get(sensorId);

        if (datos == null) {
            datos = new ArrayList<>();
        }
        if (datos.size() >= NUM_DATOS) {
            datos.remove(0); // ventana deslizante
        }
        datos.add(velocidad);
        datosVelocidad.put(sensorId, datos);

        if (datos.size() == NUM_DATOS) {
            double media = datos.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
            double max   = datos.stream().mapToDouble(Double::doubleValue).max().getAsDouble();
            double min   = datos.stream().mapToDouble(Double::doubleValue).min().getAsDouble();
            double dist  = max - min;
            return new Estadisticas(sensorId, media, max, min, dist, true);
        }

        return new Estadisticas(sensorId, 0, 0, 0, 0, false);
    }
}
