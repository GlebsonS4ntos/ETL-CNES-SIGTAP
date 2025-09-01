package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Cnes.Profissional;
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
public class ProfissionalStepConfig {

    private final DownloadProperties downloadProperties;

    public ProfissionalStepConfig(DownloadProperties downloadProperties) {
        this.downloadProperties = downloadProperties;
    }

    @Bean
    public Step profissionalStep(JobRepository repository, PlatformTransactionManager transactionManager,
                              FlatFileItemReader<Profissional> profissionalReader,
                              JdbcBatchItemWriter<Profissional> profissionalWriter,
                              TaskExecutor virtualTaskExecutor){
        return new StepBuilder("profissionalStep", repository)
                .<Profissional, Profissional>chunk(1000, transactionManager)
                .reader(profissionalReader)
                .writer(profissionalWriter)
                .taskExecutor(virtualTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Profissional> profissionalFlatFileItemReader(@Value("#{jobExecutionContext['pathCnes']}") String pathCnes){
        return new FlatFileItemReaderBuilder<Profissional>()
                .name("profissionalReader")
                .resource(new FileSystemResource(pathCnes + "/tbDadosProfissionalSus" + downloadProperties.getCnesCompetence() + ".csv"))
                .delimited()
                .delimiter(";")
                .quoteCharacter('"')
                .names("CO_PROFISSIONAL_SUS","'CO_CPF'","NO_PROFISSIONAL","CO_CNS","TO_CHAR(DT_ATUALIZACAO,'DD/MM/YYYY')","CO_USUARIO","ST_NMPROF_CADSUS","CO_NACIONALIDADE","CO_SEQ_INCLUSAO","TO_CHAR(DT_ATUALIZACAO_ORIGEM,'DD/MM/YYYY')","NO_SOCIAL")
                .fieldSetMapper(fieldSet -> new Profissional(
                        fieldSet.readString("CO_PROFISSIONAL_SUS"),
                        fieldSet.readString("NO_PROFISSIONAL"),
                        fieldSet.readString("CO_CNS")
                ))
                .linesToSkip(1)
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Profissional> profissionalWriter(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Profissional>()
                .dataSource(dataSource)
                .sql("""
                    INSERT INTO profissional (
                        coProfissional,
                        noProfissional,
                        cns
                    ) VALUES (
                        :coProfissional,
                        :noProfissional,
                        :cns
                    )
                    """)
                .beanMapped()
                .build();
    }

}