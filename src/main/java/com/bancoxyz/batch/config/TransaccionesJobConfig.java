package com.bancoxyz.batch.config;

import java.math.BigDecimal;
import java.time.LocalDate;

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

import com.bancoxyz.batch.model.Transaccion;
import com.bancoxyz.batch.processor.TransaccionProcessor;

@Configuration
public class TransaccionesJobConfig {

    @Bean
    public FlatFileItemReader<Transaccion> transaccionReader() {
        return new FlatFileItemReaderBuilder<Transaccion>()
                .name("transaccionReader")
                .resource(new ClassPathResource("data/transacciones.csv"))
                .linesToSkip(1)
                .delimited()
                .names("id", "fecha", "monto", "tipo")
                .fieldSetMapper(fieldSet -> {
                    Transaccion transaccion = new Transaccion();

                    transaccion.setId(fieldSet.readLong("id"));

                    String fecha = fieldSet.readString("fecha");
                    if (fecha != null && !fecha.isBlank()) {
                        transaccion.setFecha(LocalDate.parse(fecha.trim()));
                    }

                    String monto = fieldSet.readString("monto");
                    if (monto != null && !monto.isBlank()) {
                        transaccion.setMonto(new BigDecimal(monto.trim()));
                    }

                    transaccion.setTipo(fieldSet.readString("tipo"));

                    return transaccion;
                })
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Transaccion> transaccionWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaccion>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO transacciones_procesadas
                        (id, fecha, monto, tipo, anomala, motivo_anomalia)
                        VALUES
                        (:id, :fecha, :monto, :tipo, :anomala, :motivoAnomalia)
                        """)
                .beanMapped()
                .build();
    }

    @Bean
    public Step limpiarTransaccionesStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcTemplate jdbcTemplate) {

        return new StepBuilder("limpiarTransaccionesStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    jdbcTemplate.update("DELETE FROM resumen_transacciones");
                    jdbcTemplate.update("DELETE FROM transacciones_procesadas");

                    System.out.println("Datos anteriores de transacciones eliminados.");

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step procesarTransaccionesStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<Transaccion> transaccionReader,
            TransaccionProcessor transaccionProcessor,
            JdbcBatchItemWriter<Transaccion> transaccionWriter) {

        return new StepBuilder("procesarTransaccionesStep", jobRepository)
                .<Transaccion, Transaccion>chunk(5)
                .transactionManager(transactionManager)
                .reader(transaccionReader)
                .processor(transaccionProcessor)
                .writer(transaccionWriter)
                .build();
    }

    @Bean
    public Step resumenTransaccionesStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcTemplate jdbcTemplate) {

        return new StepBuilder("resumenTransaccionesStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    Integer totalProcesadas = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM transacciones_procesadas",
                            Integer.class);

                    Integer totalAnomalas = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM transacciones_procesadas WHERE anomala = TRUE",
                            Integer.class);

                    Integer totalValidas = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM transacciones_procesadas WHERE anomala = FALSE",
                            Integer.class);

                    BigDecimal totalCreditos = jdbcTemplate.queryForObject(
                            """
                            SELECT COALESCE(SUM(monto), 0)
                            FROM transacciones_procesadas
                            WHERE anomala = FALSE
                            AND tipo = 'credito'
                            """,
                            BigDecimal.class);

                    BigDecimal totalDebitos = jdbcTemplate.queryForObject(
                            """
                            SELECT COALESCE(SUM(monto), 0)
                            FROM transacciones_procesadas
                            WHERE anomala = FALSE
                            AND tipo = 'debito'
                            """,
                            BigDecimal.class);

                    jdbcTemplate.update(
                            """
                            INSERT INTO resumen_transacciones
                            (total_procesadas, total_validas, total_anomalas,
                             total_creditos, total_debitos)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            totalProcesadas,
                            totalValidas,
                            totalAnomalas,
                            totalCreditos,
                            totalDebitos);

                    System.out.println();
                    System.out.println("======================================");
                    System.out.println(" RESUMEN JOB TRANSACCIONES DIARIAS");
                    System.out.println("======================================");
                    System.out.println("Total procesadas : " + totalProcesadas);
                    System.out.println("Total validas    : " + totalValidas);
                    System.out.println("Total anomalas   : " + totalAnomalas);
                    System.out.println("Total creditos   : " + totalCreditos);
                    System.out.println("Total debitos    : " + totalDebitos);
                    System.out.println("======================================");
                    System.out.println();

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Job transaccionesJob(
            JobRepository jobRepository,
            @Qualifier("limpiarTransaccionesStep") Step limpiarTransaccionesStep,
            @Qualifier("procesarTransaccionesStep") Step procesarTransaccionesStep,
            @Qualifier("resumenTransaccionesStep") Step resumenTransaccionesStep) {

        return new JobBuilder("transaccionesJob", jobRepository)
                .start(limpiarTransaccionesStep)
                .next(procesarTransaccionesStep)
                .next(resumenTransaccionesStep)
                .build();
    }
}