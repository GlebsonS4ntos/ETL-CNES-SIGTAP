package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Sigtap.CompatibilidadeProcedimentosCBO;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class CompatibilidadeProcedimentosCBOStepConfig {

    @Bean
    public Step compatibilidadeProcedimentosCBOStep(JobRepository repository, PlatformTransactionManager transactionManager,
                                                    FlatFileItemReader<CompatibilidadeProcedimentosCBO> compatibilidadeProcedimentosCBOFlatFileItemReader,
                                                    JdbcBatchItemWriter<CompatibilidadeProcedimentosCBO> compatibilidadeProcedimentosCBOJdbcBatchItemWriter,
                                                    TaskExecutor virtualTaskExecutor){
        return new StepBuilder("compatibilidadeProcedimentosCBOStep", repository)
                .<CompatibilidadeProcedimentosCBO, CompatibilidadeProcedimentosCBO>chunk(1000, transactionManager)
                .reader(compatibilidadeProcedimentosCBOFlatFileItemReader)
                .writer(compatibilidadeProcedimentosCBOJdbcBatchItemWriter)
                .taskExecutor(virtualTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<CompatibilidadeProcedimentosCBO> compatibilidadeProcedimentosCBOReader(@Value("#{jobExecutionContext['pathSigtap']}") String sigtapPath){
        return new FlatFileItemReaderBuilder<CompatibilidadeProcedimentosCBO>()
                .name("compatibilidadeProcedimentosCBOReader")
                .resource(new FileSystemResource(sigtapPath + "/rl_procedimento_ocupacao.txt"))
                .lineTokenizer(lengthColumns())
                .fieldSetMapper(fieldSet -> new CompatibilidadeProcedimentosCBO(
                        fieldSet.readString("codigoProcedimento"),
                        fieldSet.readString("codigoCBO"),
                        fieldSet.readString("competencia")
                )).build();
    }

    private LineTokenizer lengthColumns() {
        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();

        tokenizer.setNames(
                "codigoProcedimento",
                "codigoCBO",
                "competencia"
        );

        tokenizer.setColumns(
                new Range(1, 10),
                new Range(11, 16),
                new Range(17, 22)
        );

        return tokenizer;
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<CompatibilidadeProcedimentosCBO> compatibilidadeProcedimentosCBOWriter(@Qualifier("dataSource") DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<CompatibilidadeProcedimentosCBO>()
                .dataSource(dataSource)
                .sql("""
                INSERT INTO compatibilidade_procedimentos_cbo (
                    codigo_procedimento,
                    codigo_cbo,
                    competencia
                ) VALUES (
                    :codigoProcedimento,
                    :codigoCBO,
                    :competencia
                )
                """)
                .beanMapped()
                .build();
    }

}