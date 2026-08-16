package com.bancoxyz.batch.processor;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.bancoxyz.batch.model.CuentaInteres;

@Component
public class CuentaInteresProcessor
        implements ItemProcessor<CuentaInteres, CuentaInteres> {

    private static final BigDecimal TASA_AHORRO =
            new BigDecimal("0.01");

    private static final BigDecimal TASA_PRESTAMO =
            new BigDecimal("0.02");

    @Override
    public CuentaInteres process(CuentaInteres cuenta) {

        StringBuilder errores = new StringBuilder();

        // Normalizar datos
        if (cuenta.getNombre() != null) {
            cuenta.setNombre(cuenta.getNombre().trim());
        }

        if (cuenta.getTipo() != null) {
            cuenta.setTipo(cuenta.getTipo().trim().toLowerCase());
        }

        // Validar nombre
        if (cuenta.getNombre() == null || cuenta.getNombre().isBlank()) {
            agregarError(errores, "Nombre faltante");
        }

        // Validar saldo
        if (cuenta.getSaldo() == null) {
            agregarError(errores, "Saldo faltante");
        } else if (cuenta.getSaldo().compareTo(BigDecimal.ZERO) <= 0) {
            agregarError(errores, "Saldo debe ser mayor a cero");
        }

        // Validar edad
        if (cuenta.getEdad() == null
                || cuenta.getEdad() <= 0
                || cuenta.getEdad() > 120) {
            agregarError(errores, "Edad no valida");
        }

        // Determinar tasa según tipo
        if ("ahorro".equals(cuenta.getTipo())) {

            cuenta.setTasaInteres(TASA_AHORRO);

        } else if ("prestamo".equals(cuenta.getTipo())) {

            cuenta.setTasaInteres(TASA_PRESTAMO);

        } else {

            agregarError(
                    errores,
                    "Tipo de cuenta no valido para calculo de intereses");

            cuenta.setTasaInteres(BigDecimal.ZERO);
        }

        // Si existen errores, no calcular interés
        if (errores.length() > 0) {

            cuenta.setValida(false);
            cuenta.setMotivoError(errores.toString());
            cuenta.setInteresCalculado(BigDecimal.ZERO);
            cuenta.setSaldoFinal(cuenta.getSaldo());

            return cuenta;
        }

        // Calcular interés
        BigDecimal interes = cuenta.getSaldo()
                .multiply(cuenta.getTasaInteres())
                .setScale(2, RoundingMode.HALF_UP);

        BigDecimal saldoFinal = cuenta.getSaldo()
                .add(interes)
                .setScale(2, RoundingMode.HALF_UP);

        cuenta.setInteresCalculado(interes);
        cuenta.setSaldoFinal(saldoFinal);
        cuenta.setValida(true);
        cuenta.setMotivoError(null);

        return cuenta;
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