package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Cnes.Cbo;
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
public class CboStepConfig {

    private final DownloadProperties downloadProperties;

    public CboStepConfig(DownloadProperties downloadProperties) {
        this.downloadProperties = downloadProperties;
    }

    @Bean
    public Step cboStep(JobRepository repository, PlatformTransactionManager transactionManager,
                           FlatFileItemReader<Cbo> cboReader,
                           JdbcBatchItemWriter<Cbo> cboWriter,
                           TaskExecutor virtualTaskExecutor){
        return new StepBuilder("cboStep", repository)
                .<Cbo, Cbo>chunk(1000, transactionManager)
                .reader(cboReader)
                .writer(cboWriter)
                .taskExecutor(virtualTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Cbo> cboFlatFileItemReader(@Value("#{jobExecutionContext['pathCnes']}") String pathCnes){
        return new FlatFileItemReaderBuilder<Cbo>()
                .name("cboReader")
                .resource(new FileSystemResource(pathCnes + "/tbAtividadeProfissional" + downloadProperties.getCnesCompetence() + ".csv"))
                .delimited()
                .delimiter(";")
                .quoteCharacter('"')
                .names("CO_CBO","DS_ATIVIDADE_PROFISSIONAL","TP_CLASSIFICACAO_PROFISSIONAL","TP_CBO_SAUDE","ST_CBO_REGULAMENTADO","NO_ANO_CMPT")
                .fieldSetMapper(fieldSet -> new Cbo(
                        fieldSet.readString("CO_CBO"),
                        fieldSet.readString("DS_ATIVIDADE_PROFISSIONAL").trim()
                ))
                .linesToSkip(1)
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Cbo> cboWriter(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Cbo>()
                .dataSource(dataSource)
                .sql("""
                    INSERT INTO cbo (
                        coCbo,
                        noAtividadeProfissional
                    ) VALUES (
                        :coCbo,
                        :noAtividadeProfissional
                    )
                    """)
                .beanMapped()
                .build();
    }

}


