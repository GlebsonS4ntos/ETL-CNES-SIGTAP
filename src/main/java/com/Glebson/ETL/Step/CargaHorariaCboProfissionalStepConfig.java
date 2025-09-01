package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Cnes.CargaHorariaCboProfissional;
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
public class CargaHorariaCboProfissionalStepConfig {
    private final DownloadProperties downloadProperties;

    public CargaHorariaCboProfissionalStepConfig(DownloadProperties downloadProperties) {
        this.downloadProperties = downloadProperties;
    }

    @Bean
    public Step cargaHorariaCboProfissionalStep(JobRepository repository, PlatformTransactionManager transactionManager,
                              FlatFileItemReader<CargaHorariaCboProfissional> cargaHorariaCboProfissionalReader,
                              JdbcBatchItemWriter<CargaHorariaCboProfissional> cargaHorariaCboProfissionalWriter,
                              TaskExecutor virtualTaskExecutor){
        return new StepBuilder("qtdCargaHorariaStep", repository)
                .<CargaHorariaCboProfissional, CargaHorariaCboProfissional>chunk(1000, transactionManager)
                .reader(cargaHorariaCboProfissionalReader)
                .writer(cargaHorariaCboProfissionalWriter)
                .taskExecutor(virtualTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<CargaHorariaCboProfissional> cargaHorariaCboProfissionalFlatFileItemReader(@Value("#{jobExecutionContext['pathCnes']}") String pathCnes){
        return new FlatFileItemReaderBuilder<CargaHorariaCboProfissional>()
                .name("cargaHorariaCboProfissionalReader")
                .resource(new FileSystemResource(pathCnes + "/tbCargaHorariaSus" + downloadProperties.getCnesCompetence() + ".csv"))
                .delimited()
                .delimiter(";")
                .quoteCharacter('"')
                .names("CO_UNIDADE","CO_PROFISSIONAL_SUS","CO_CBO","TP_SUS_NAO_SUS","IND_VINCULACAO","TP_TERCEIRO_SIH","QT_CARGA_HORARIA_AMBULATORIAL","CO_CONSELHO_CLASSE","NU_REGISTRO","SG_UF_CRM","TP_PRECEPTOR","TP_RESIDENTE","NU_CNPJ_DETALHAMENTO_VINCULO","TO_CHAR(A.DT_ATUALIZACAO,'DD/MM/YYYY')","CO_USUARIO","TO_CHAR(A.DT_ATUALIZACAO_ORIGEM,'DD/MM/YYYY')","QT_CARGA_HORARIA_OUTROS","QT_CARGA_HOR_HOSP_SUS")
                .fieldSetMapper(fieldSet -> new CargaHorariaCboProfissional(
                        fieldSet.readString("CO_UNIDADE"),
                        fieldSet.readString("CO_PROFISSIONAL_SUS"),
                        fieldSet.readString("CO_CBO"),
                        fieldSet.readString("QT_CARGA_HORARIA_AMBULATORIAL")
                ))
                .linesToSkip(1)
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<CargaHorariaCboProfissional> cargaHorariaCboProfissionalWriter(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<CargaHorariaCboProfissional>()
                .dataSource(dataSource)
                .sql("""
                    INSERT INTO cargaHorariaCboProfissional (
                        coUnidade,
                        coProfissional,
                        coCbo,
                        qtdCargaHoraria
                    ) VALUES (
                        :coUnidade,
                        :coProfissional,
                        :coCbo,
                        :qtdCargaHoraria
                    )
                    """)
                .beanMapped()
                .build();
    }
}