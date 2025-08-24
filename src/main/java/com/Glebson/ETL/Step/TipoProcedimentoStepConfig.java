package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Sigtap.TipoProcedimento;
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
public class TipoProcedimentoStepConfig {

    @Bean
    public Step tipoProcedimentoStep(JobRepository repository, PlatformTransactionManager transactionManager,
                                     FlatFileItemReader<TipoProcedimento> tipoProcedimentoReader,
                                     JdbcBatchItemWriter<TipoProcedimento> tipoProcedimentoJdbcBatchItemWriter,
                                     TaskExecutor virtualTaskExecutor){
        return new StepBuilder("tipoProcedimentoStep", repository)
                .<TipoProcedimento, TipoProcedimento>chunk(1000, transactionManager)
                .reader(tipoProcedimentoReader)
                .writer(tipoProcedimentoJdbcBatchItemWriter)
                .taskExecutor(virtualTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<TipoProcedimento> tipoProcedimentoReader(@Value("#{jobExecutionContext['pathSigtap']}") String sigtapPath){
        return new FlatFileItemReaderBuilder<TipoProcedimento>()
                .name("tipoProcedimentoReader")
                .resource(new FileSystemResource(sigtapPath + "/tb_registro.txt"))
                .lineTokenizer(lengthColumns())
                .fieldSetMapper(fieldSet -> new TipoProcedimento(
                        fieldSet.readString("codigoTipo"),
                        fieldSet.readString("nomeTipo").trim(),
                        fieldSet.readString("competencia")
                )).build();
    }

    private LineTokenizer lengthColumns() {
        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();

        tokenizer.setNames(
                "codigoTipo",
                "nomeTipo",
                "competencia"
        );

        tokenizer.setColumns(
                new Range(1, 2),
                new Range(3, 52),
                new Range(53, 58)
        );

        return tokenizer;
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<TipoProcedimento> tipoProcedimentoWriter(@Qualifier("dataSource") DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<TipoProcedimento>()
                .dataSource(dataSource)
                .sql("""
                INSERT INTO tipo_procedimento (
                    codigo_tipo,
                    nome_tipo,
                    competencia
                ) VALUES (
                    :codigoTipo,
                    :nomeTipo,
                    :competencia
                )
                """)
                .beanMapped()
                .build();
    }
}