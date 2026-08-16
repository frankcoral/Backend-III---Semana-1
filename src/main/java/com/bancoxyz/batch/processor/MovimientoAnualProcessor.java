package com.bancoxyz.batch.processor;

import java.math.BigDecimal;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.bancoxyz.batch.model.MovimientoAnual;

@Component
public class MovimientoAnualProcessor
        implements ItemProcessor<MovimientoAnual, MovimientoAnual> {

    @Override
    public MovimientoAnual process(MovimientoAnual movimiento) {

        StringBuilder errores = new StringBuilder();

        if (movimiento.getTransaccion() != null) {
            movimiento.setTransaccion(
                    movimiento.getTransaccion().trim().toLowerCase());
        }

        if (movimiento.getDescripcion() != null) {
            movimiento.setDescripcion(
                    movimiento.getDescripcion().trim());
        }

        // Fecha obligatoria
        if (movimiento.getFecha() == null) {
            agregarError(errores, "Fecha faltante o invalida");
        }

        // Descripción obligatoria
        if (movimiento.getDescripcion() == null
                || movimiento.getDescripcion().isBlank()) {
            agregarError(errores, "Descripcion faltante");
        }

        // Monto obligatorio y diferente de cero
        if (movimiento.getMonto() == null) {

            agregarError(errores, "Monto faltante");

        } else if (movimiento.getMonto()
                .compareTo(BigDecimal.ZERO) == 0) {

            agregarError(errores, "Monto no puede ser cero");
        }

        // Validar tipo de transacción
        String tipo = movimiento.getTransaccion();

        if (tipo == null || tipo.isBlank()) {

            agregarError(errores, "Tipo de transaccion faltante");

        } else if (!tipo.equals("deposito")
                && !tipo.equals("retiro")
                && !tipo.equals("compra")) {

            agregarError(errores, "Tipo de transaccion no valido");
        }

        // Validar coherencia entre tipo y signo del monto
        if (movimiento.getMonto() != null) {

            if ("deposito".equals(tipo)
                    && movimiento.getMonto()
                    .compareTo(BigDecimal.ZERO) <= 0) {

                agregarError(
                        errores,
                        "Deposito debe tener monto positivo");
            }

            if (("retiro".equals(tipo) || "compra".equals(tipo))
                    && movimiento.getMonto()
                    .compareTo(BigDecimal.ZERO) >= 0) {

                agregarError(
                        errores,
                        "Retiro o compra debe tener monto negativo");
            }
        }

        if (errores.length() > 0) {
            movimiento.setValido(false);
            movimiento.setMotivoError(errores.toString());
        } else {
            movimiento.setValido(true);
            movimiento.setMotivoError(null);
        }

        return movimiento;
    }

    private void agregarError(
            StringBuilder errores,
            String mensaje) {

        if (errores.length() > 0) {
            errores.append("; ");
        }

        errores.append(mensaje);
    }
}