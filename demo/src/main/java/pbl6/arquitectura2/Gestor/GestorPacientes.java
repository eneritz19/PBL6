package pacientes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Gestiona la ventana deslizante de pulsaciones por paciente.
 * Todos sus accesos son sincronizados: es seguro usarlo desde
 * múltiples hilos del Thread Pool simultáneamente.
 */
public class GestorPacientes {

    static final int NUM_DATOS = 5;

    Map<String, List<Integer>> datosPacientes;

    public GestorPacientes() {
        datosPacientes = new HashMap<>();
    }

    /**
     * Añade la nueva medición al historial del paciente y devuelve
     * la media de las últimas NUM_DATOS lecturas (0 si aún no hay
     * suficientes datos).
     */
    public double getMedia(String id, int pulsaciones) {
        synchronized (datosPacientes) {
            List<Integer> datosPaciente = datosPacientes.get(id);
            double media = 0;
            if (datosPaciente == null) {
                datosPaciente = new ArrayList<>();
            }
            if (datosPaciente.size() >= NUM_DATOS) {
                datosPaciente.remove(0);
            }
            datosPaciente.add(pulsaciones);
            datosPacientes.put(id, datosPaciente);
            if (datosPaciente.size() == NUM_DATOS) {
                media = datosPaciente.stream()
                        .mapToInt(Integer::intValue)
                        .average()
                        .getAsDouble();
            }
            return media;
        }
    }
}
