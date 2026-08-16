package com.bancoxyz.batch.processor;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.bancoxyz.batch.model.Transaccion;

@Component
public class TransaccionProcessor implements ItemProcessor<Transaccion, Transaccion> {

    private final Set<String> transaccionesProcesadas = new HashSet<>();

    @Override
    public Transaccion process(Transaccion transaccion) {

        StringBuilder motivos = new StringBuilder();

        // Normalizar el tipo de transacción
        if (transaccion.getTipo() != null) {
            transaccion.setTipo(
                transaccion.getTipo().trim().toLowerCase()
            );
        }

        // Validar fecha
        if (transaccion.getFecha() == null) {
            agregarMotivo(motivos, "Fecha invalida o faltante");
        }

        // Validar monto
        if (transaccion.getMonto() == null) {
            agregarMotivo(motivos, "Monto faltante");
        } else if (transaccion.getMonto().compareTo(BigDecimal.ZERO) <= 0) {
            agregarMotivo(motivos, "Monto negativo o cero");
        }

        // Validar tipo
        if (transaccion.getTipo() == null || transaccion.getTipo().isBlank()) {
            agregarMotivo(motivos, "Tipo de transaccion faltante");
        } else if (!transaccion.getTipo().equals("debito")
                && !transaccion.getTipo().equals("credito")) {
            agregarMotivo(motivos, "Tipo de transaccion no valido");
        }

        // Detectar registros duplicados según fecha, monto y tipo
        if (transaccion.getFecha() != null
                && transaccion.getMonto() != null
                && transaccion.getTipo() != null) {

            String firma = transaccion.getFecha()
                    + "|"
                    + transaccion.getMonto().stripTrailingZeros().toPlainString()
                    + "|"
                    + transaccion.getTipo();

            if (!transaccionesProcesadas.add(firma)) {
                agregarMotivo(motivos, "Transaccion duplicada");
            }
        }

        // Registrar resultado de las validaciones
        if (motivos.length() > 0) {
            transaccion.setAnomala(true);
            transaccion.setMotivoAnomalia(motivos.toString());
        } else {
            transaccion.setAnomala(false);
            transaccion.setMotivoAnomalia(null);
        }

        return transaccion;
    }

    private void agregarMotivo(StringBuilder motivos, String motivo) {
        if (motivos.length() > 0) {
            motivos.append("; ");
        }
        motivos.append(motivo);
    }
}