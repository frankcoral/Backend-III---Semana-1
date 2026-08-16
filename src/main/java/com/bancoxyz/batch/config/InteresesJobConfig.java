package com.bancoxyz.batch.config;

import java.math.BigDecimal;

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

import com.bancoxyz.batch.model.CuentaInteres;
import com.bancoxyz.batch.processor.CuentaInteresProcessor;

@Configuration
public class InteresesJobConfig {

    @Bean
    public FlatFileItemReader<CuentaInteres> cuentaInteresReader() {

        return new FlatFileItemReaderBuilder<CuentaInteres>()
                .name("cuentaInteresReader")
                .resource(new ClassPathResource("data/intereses.csv"))
                .linesToSkip(1)
                .delimited()
                .names("cuenta_id", "nombre", "saldo", "edad", "tipo")
                .fieldSetMapper(fieldSet -> {

                    CuentaInteres cuenta = new CuentaInteres();

                    cuenta.setCuentaId(
                            fieldSet.readLong("cuenta_id"));

                    cuenta.setNombre(
                            fieldSet.readString("nombre"));

                    String saldo = fieldSet.readString("saldo");

                    if (saldo != null && !saldo.isBlank()) {
                        cuenta.setSaldo(
                                new BigDecimal(saldo.trim()));
                    }

                    String edad = fieldSet.readString("edad");

                    if (edad != null && !edad.isBlank()) {
                        cuenta.setEdad(
                                Integer.valueOf(edad.trim()));
                    }

                    cuenta.setTipo(
                            fieldSet.readString("tipo"));

                    return cuenta;
                })
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<CuentaInteres> cuentaInteresWriter(
            DataSource dataSource) {

        return new JdbcBatchItemWriterBuilder<CuentaInteres>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO intereses_procesados
                        (
                            cuenta_id,
                            nombre,
                            saldo_inicial,
                            edad,
                            tipo,
                            tasa_interes,
                            interes_calculado,
                            saldo_final,
                            valida,
                            motivo_error
                        )
                        VALUES
                        (
                            :cuentaId,
                            :nombre,
                            :saldo,
                            :edad,
                            :tipo,
                            :tasaInteres,
                            :interesCalculado,
                            :saldoFinal,
                            :valida,
                            :motivoError
                        )
                        """)
                .beanMapped()
                .build();
    }

    @Bean
    public Step limpiarInteresesStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcTemplate jdbcTemplate) {

        return new StepBuilder(
                "limpiarInteresesStep",
                jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    jdbcTemplate.update(
                            "DELETE FROM resumen_intereses");

                    jdbcTemplate.update(
                            "DELETE FROM intereses_procesados");

                    System.out.println(
                            "Datos anteriores de intereses eliminados.");

                    return RepeatStatus.FINISHED;

                }, transactionManager)
                .build();
    }

    @Bean
    public Step procesarInteresesStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<CuentaInteres> cuentaInteresReader,
            CuentaInteresProcessor cuentaInteresProcessor,
            JdbcBatchItemWriter<CuentaInteres> cuentaInteresWriter) {

        return new StepBuilder(
                "procesarInteresesStep",
                jobRepository)
                .<CuentaInteres, CuentaInteres>chunk(5)
                .transactionManager(transactionManager)
                .reader(cuentaInteresReader)
                .processor(cuentaInteresProcessor)
                .writer(cuentaInteresWriter)
                .build();
    }

    @Bean
    public Step resumenInteresesStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcTemplate jdbcTemplate) {

        return new StepBuilder(
                "resumenInteresesStep",
                jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    Integer totalProcesadas =
                            jdbcTemplate.queryForObject(
                                    """
                                    SELECT COUNT(*)
                                    FROM intereses_procesados
                                    """,
                                    Integer.class);

                    Integer totalValidas =
                            jdbcTemplate.queryForObject(
                                    """
                                    SELECT COUNT(*)
                                    FROM intereses_procesados
                                    WHERE valida = TRUE
                                    """,
                                    Integer.class);

                    Integer totalInvalidas =
                            jdbcTemplate.queryForObject(
                                    """
                                    SELECT COUNT(*)
                                    FROM intereses_procesados
                                    WHERE valida = FALSE
                                    """,
                                    Integer.class);

                    BigDecimal interesTotal =
                            jdbcTemplate.queryForObject(
                                    """
                                    SELECT COALESCE(
                                        SUM(interes_calculado), 0
                                    )
                                    FROM intereses_procesados
                                    WHERE valida = TRUE
                                    """,
                                    BigDecimal.class);

                    jdbcTemplate.update(
                            """
                            INSERT INTO resumen_intereses
                            (
                                total_procesadas,
                                total_validas,
                                total_invalidas,
                                interes_total
                            )
                            VALUES (?, ?, ?, ?)
                            """,
                            totalProcesadas,
                            totalValidas,
                            totalInvalidas,
                            interesTotal);

                    System.out.println();
                    System.out.println(
                            "======================================");
                    System.out.println(
                            " RESUMEN JOB INTERESES MENSUALES");
                    System.out.println(
                            "======================================");
                    System.out.println(
                            "Total procesadas : " + totalProcesadas);
                    System.out.println(
                            "Total validas    : " + totalValidas);
                    System.out.println(
                            "Total invalidas  : " + totalInvalidas);
                    System.out.println(
                            "Interes total    : " + interesTotal);
                    System.out.println(
                            "======================================");
                    System.out.println();

                    return RepeatStatus.FINISHED;

                }, transactionManager)
                .build();
    }

    @Bean
    public Job interesesJob(
            JobRepository jobRepository,
            @Qualifier("limpiarInteresesStep")
            Step limpiarInteresesStep,
            @Qualifier("procesarInteresesStep")
            Step procesarInteresesStep,
            @Qualifier("resumenInteresesStep")
            Step resumenInteresesStep) {

        return new JobBuilder(
                "interesesJob",
                jobRepository)
                .start(limpiarInteresesStep)
                .next(procesarInteresesStep)
                .next(resumenInteresesStep)
                .build();
    }
}