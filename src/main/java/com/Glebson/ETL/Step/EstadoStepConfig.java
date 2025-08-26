package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Cnes.Estado;
import com.Glebson.ETL.Utils.DownloadProperties;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class EstadoStepConfig {

    private final DownloadProperties downloadProperties;

    public EstadoStepConfig(DownloadProperties downloadProperties) {
        this.downloadProperties = downloadProperties;
    }

    @Bean
    public Step estadoStep(JobRepository repository, PlatformTransactionManager transactionManager,
                                 FlatFileItemReader<Estado> estadoReader,
                                 JdbcBatchItemWriter<Estado> estadoWriter,
                                 TaskExecutor virtualTaskExecutor){
        return new StepBuilder("estadoStep", repository)
                .<Estado, Estado>chunk(1000, transactionManager)
                .reader(estadoReader)
                .writer(estadoWriter)
                .taskExecutor(virtualTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Estado> estadoFlatFileItemReader(@Value("#{jobExecutionContext['pathCnes']}") String pathCnes){
        return new FlatFileItemReaderBuilder<Estado>()
                .name("estadoReader")
                .resource(new FileSystemResource(pathCnes + "/tbEstado" + downloadProperties.getCnesCompetence() + ".csv"))
                .delimited()
                .delimiter(";")
                .quoteCharacter('"')
                .names("CO_UF", "CO_SIGLA", "NO_DESCRICAO")
                .fieldSetMapper(fieldSet -> new Estado(
                        fieldSet.readString("CO_UF"),
                        fieldSet.readString("CO_SIGLA"),
                        fieldSet.readString("NO_DESCRICAO").trim()
                ))
                .linesToSkip(1)
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Estado> estadoWriter(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Estado>()
                .dataSource(dataSource)
                .sql("""
                    INSERT INTO estado (
                        coUf,
                        coSigla,
                        noEstado
                    ) VALUES (
                        :coUf,
                        :coSigla,
                        :noEstado
                    )
                    """)
                .beanMapped()
                .build();
    }
}
