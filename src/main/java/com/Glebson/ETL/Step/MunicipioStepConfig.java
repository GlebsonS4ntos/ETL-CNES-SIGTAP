package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Cnes.Municipio;
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
public class MunicipioStepConfig {

    private final DownloadProperties downloadProperties;

    public MunicipioStepConfig(DownloadProperties downloadProperties) {
        this.downloadProperties = downloadProperties;
    }

    @Bean
    public Step municipioStep(JobRepository repository, PlatformTransactionManager transactionManager,
                           FlatFileItemReader<Municipio> municipioReader,
                           JdbcBatchItemWriter<Municipio> municipioWriter,
                           TaskExecutor virtualTaskExecutor){
        return new StepBuilder("municipioStep", repository)
                .<Municipio, Municipio>chunk(1000, transactionManager)
                .reader(municipioReader)
                .writer(municipioWriter)
                .taskExecutor(virtualTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Municipio> municipioFlatFileItemReader(@Value("#{jobExecutionContext['pathCnes']}") String pathCnes){
        return new FlatFileItemReaderBuilder<Municipio>()
                .name("municipioReader")
                .resource(new FileSystemResource(pathCnes + "/tbMunicipio" + downloadProperties.getCnesCompetence() + ".csv"))
                .delimited()
                .delimiter(";")
                .quoteCharacter('"')
                .names("CO_MUNICIPIO","NO_MUNICIPIO","CO_SIGLA_ESTADO","TP_CADASTRO","TP_PACTO","TP_ENVIA","TP_ENVIA_CNES","TP_CIB_SAS","TP_PLENO_ORIGEM","TP_MAC","NU_POPULACAO","NU_DENSIDADE","CMTP_INICIO_MAC")
                .fieldSetMapper(fieldSet -> new Municipio(
                        fieldSet.readString("CO_MUNICIPIO"),
                        fieldSet.readString("NO_MUNICIPIO"),
                        fieldSet.readString("CO_SIGLA_ESTADO")
                ))
                .linesToSkip(1)
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Municipio> municipioWriter(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Municipio>()
                .dataSource(dataSource)
                .sql("""
                    INSERT INTO municipio (
                        coMunicipio,
                        noMunicipio,
                        coSigla
                    ) VALUES (
                        :coMunicipio,
                        :noMunicipio,
                        :coSigla
                    )
                    """)
                .beanMapped()
                .build();
    }

}
