package com.bancoxyz.batch.config;

import java.io.BufferedWriter;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.bancoxyz.batch.model.MovimientoAnual;
import com.bancoxyz.batch.processor.MovimientoAnualProcessor;

@Configuration
public class EstadosCuentaJobConfig {

    @Bean
    public FlatFileItemReader<MovimientoAnual> movimientoAnualReader() {

        return new FlatFileItemReaderBuilder<MovimientoAnual>()
                .name("movimientoAnualReader")
                .resource(new ClassPathResource("data/cuentas_anuales.csv"))
                .linesToSkip(1)
                .delimited()
                .names(
                        "cuenta_id",
                        "fecha",
                        "transaccion",
                        "monto",
                        "descripcion")
                .fieldSetMapper(fieldSet -> {

                    MovimientoAnual movimiento =
                            new MovimientoAnual();

                    movimiento.setCuentaId(
                            fieldSet.readLong("cuenta_id"));

                    String fecha =
                            fieldSet.readString("fecha");

                    if (fecha != null && !fecha.isBlank()) {
                        movimiento.setFecha(
                                LocalDate.parse(fecha.trim()));
                    }

                    movimiento.setTransaccion(
                            fieldSet.readString("transaccion"));

                    String monto =
                            fieldSet.readString("monto");

                    if (monto != null && !monto.isBlank()) {
                        movimiento.setMonto(
                                new BigDecimal(monto.trim()));
                    }

                    movimiento.setDescripcion(
                            fieldSet.readString("descripcion"));

                    return movimiento;
                })
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<MovimientoAnual> movimientoAnualWriter(
            DataSource dataSource) {

        return new JdbcBatchItemWriterBuilder<MovimientoAnual>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO movimientos_anuales
                        (
                            cuenta_id,
                            fecha,
                            transaccion,
                            monto,
                            descripcion,
                            valido,
                            motivo_error
                        )
                        VALUES
                        (
                            :cuentaId,
                            :fecha,
                            :transaccion,
                            :monto,
                            :descripcion,
                            :valido,
                            :motivoError
                        )
                        """)
                .beanMapped()
                .build();
    }

    @Bean
    public Step limpiarEstadosCuentaStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcTemplate jdbcTemplate) {

        return new StepBuilder(
                "limpiarEstadosCuentaStep",
                jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    jdbcTemplate.update(
                            "DELETE FROM estados_cuenta_anuales");

                    jdbcTemplate.update(
                            "DELETE FROM movimientos_anuales");

                    System.out.println(
                            "Datos anteriores de estados de cuenta eliminados.");

                    return RepeatStatus.FINISHED;

                }, transactionManager)
                .build();
    }

    @Bean
    public Step procesarMovimientosAnualesStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<MovimientoAnual> movimientoAnualReader,
            MovimientoAnualProcessor movimientoAnualProcessor,
            JdbcBatchItemWriter<MovimientoAnual> movimientoAnualWriter) {

        return new StepBuilder(
                "procesarMovimientosAnualesStep",
                jobRepository)
                .<MovimientoAnual, MovimientoAnual>chunk(5)
                .transactionManager(transactionManager)
                .reader(movimientoAnualReader)
                .processor(movimientoAnualProcessor)
                .writer(movimientoAnualWriter)
                .build();
    }

    @Bean
    public Step generarEstadosCuentaStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcTemplate jdbcTemplate) {

        return new StepBuilder(
                "generarEstadosCuentaStep",
                jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    jdbcTemplate.update("""
                            INSERT INTO estados_cuenta_anuales
                            (
                                cuenta_id,
                                total_movimientos,
                                movimientos_validos,
                                movimientos_invalidos,
                                total_depositos,
                                total_cargos,
                                saldo_neto
                            )
                            SELECT
                                cuenta_id,
                                COUNT(*) AS total_movimientos,

                                SUM(
                                    CASE
                                        WHEN valido = TRUE
                                        THEN 1
                                        ELSE 0
                                    END
                                ) AS movimientos_validos,

                                SUM(
                                    CASE
                                        WHEN valido = FALSE
                                        THEN 1
                                        ELSE 0
                                    END
                                ) AS movimientos_invalidos,

                                COALESCE(
                                    SUM(
                                        CASE
                                            WHEN valido = TRUE
                                            AND transaccion = 'deposito'
                                            THEN monto
                                            ELSE 0
                                        END
                                    ),
                                    0
                                ) AS total_depositos,

                                COALESCE(
                                    SUM(
                                        CASE
                                            WHEN valido = TRUE
                                            AND transaccion IN ('retiro', 'compra')
                                            THEN ABS(monto)
                                            ELSE 0
                                        END
                                    ),
                                    0
                                ) AS total_cargos,

                                COALESCE(
                                    SUM(
                                        CASE
                                            WHEN valido = TRUE
                                            THEN monto
                                            ELSE 0
                                        END
                                    ),
                                    0
                                ) AS saldo_neto

                            FROM movimientos_anuales
                            GROUP BY cuenta_id
                            """);

                    return RepeatStatus.FINISHED;

                }, transactionManager)
                .build();
    }

    @Bean
    public Step exportarInformeAuditoriaStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcTemplate jdbcTemplate) {

        return new StepBuilder(
                "exportarInformeAuditoriaStep",
                jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    Path carpetaOutput =
                            Paths.get("output");

                    Files.createDirectories(carpetaOutput);

                    Path archivo =
                            carpetaOutput.resolve(
                                    "estados_cuenta_anuales.csv");

                    List<Map<String, Object>> estados =
                            jdbcTemplate.queryForList("""
                                    SELECT
                                        cuenta_id,
                                        total_movimientos,
                                        movimientos_validos,
                                        movimientos_invalidos,
                                        total_depositos,
                                        total_cargos,
                                        saldo_neto,
                                        fecha_generacion
                                    FROM estados_cuenta_anuales
                                    ORDER BY cuenta_id
                                    """);

                    try (BufferedWriter writer =
                            Files.newBufferedWriter(
                                    archivo,
                                    StandardOpenOption.CREATE,
                                    StandardOpenOption.TRUNCATE_EXISTING)) {

                        writer.write(
                                "cuenta_id,"
                                + "total_movimientos,"
                                + "movimientos_validos,"
                                + "movimientos_invalidos,"
                                + "total_depositos,"
                                + "total_cargos,"
                                + "saldo_neto,"
                                + "fecha_generacion");

                        writer.newLine();

                        for (Map<String, Object> estado : estados) {

                            writer.write(
                                    estado.get("cuenta_id") + ","
                                    + estado.get("total_movimientos") + ","
                                    + estado.get("movimientos_validos") + ","
                                    + estado.get("movimientos_invalidos") + ","
                                    + estado.get("total_depositos") + ","
                                    + estado.get("total_cargos") + ","
                                    + estado.get("saldo_neto") + ","
                                    + estado.get("fecha_generacion"));

                            writer.newLine();
                        }
                    }

                    Integer totalMovimientos =
                            jdbcTemplate.queryForObject(
                                    """
                                    SELECT COUNT(*)
                                    FROM movimientos_anuales
                                    """,
                                    Integer.class);

                    Integer movimientosValidos =
                            jdbcTemplate.queryForObject(
                                    """
                                    SELECT COUNT(*)
                                    FROM movimientos_anuales
                                    WHERE valido = TRUE
                                    """,
                                    Integer.class);

                    Integer movimientosInvalidos =
                            jdbcTemplate.queryForObject(
                                    """
                                    SELECT COUNT(*)
                                    FROM movimientos_anuales
                                    WHERE valido = FALSE
                                    """,
                                    Integer.class);

                    Integer totalCuentas =
                            jdbcTemplate.queryForObject(
                                    """
                                    SELECT COUNT(*)
                                    FROM estados_cuenta_anuales
                                    """,
                                    Integer.class);

                    System.out.println();
                    System.out.println(
                            "======================================");
                    System.out.println(
                            " RESUMEN JOB ESTADOS DE CUENTA ANUALES");
                    System.out.println(
                            "======================================");
                    System.out.println(
                            "Total movimientos : " + totalMovimientos);
                    System.out.println(
                            "Movimientos validos: " + movimientosValidos);
                    System.out.println(
                            "Mov. invalidos     : " + movimientosInvalidos);
                    System.out.println(
                            "Cuentas procesadas : " + totalCuentas);
                    System.out.println(
                            "Informe generado   : "
                            + archivo.toAbsolutePath());
                    System.out.println(
                            "======================================");
                    System.out.println();

                    return RepeatStatus.FINISHED;

                }, transactionManager)
                .build();
    }

    @Bean
    public Job estadosCuentaAnualesJob(
            JobRepository jobRepository,

            @Qualifier("limpiarEstadosCuentaStep")
            Step limpiarEstadosCuentaStep,

            @Qualifier("procesarMovimientosAnualesStep")
            Step procesarMovimientosAnualesStep,

            @Qualifier("generarEstadosCuentaStep")
            Step generarEstadosCuentaStep,

            @Qualifier("exportarInformeAuditoriaStep")
            Step exportarInformeAuditoriaStep) {

        return new JobBuilder(
                "estadosCuentaAnualesJob",
                jobRepository)
                .start(limpiarEstadosCuentaStep)
                .next(procesarMovimientosAnualesStep)
                .next(generarEstadosCuentaStep)
                .next(exportarInformeAuditoriaStep)
                .build();
    }
}