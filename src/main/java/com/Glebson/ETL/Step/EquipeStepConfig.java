package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Cnes.Equipe;
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
public class EquipeStepConfig {

    private final DownloadProperties downloadProperties;

    public EquipeStepConfig(DownloadProperties downloadProperties) {
        this.downloadProperties = downloadProperties;
    }

    @Bean
    public Step equipeStep(JobRepository repository, PlatformTransactionManager transactionManager,
                                 FlatFileItemReader<Equipe> equipeReader,
                                 JdbcBatchItemWriter<Equipe> equipeWriter,
                                 TaskExecutor virtualTaskExecutor){
        return new StepBuilder("equipeStep", repository)
                .<Equipe, Equipe>chunk(1000, transactionManager)
                .reader(equipeReader)
                .writer(equipeWriter)
                .taskExecutor(virtualTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Equipe> equipeFlatFileItemReader(@Value("#{jobExecutionContext['pathCnes']}") String pathCnes){
        return new FlatFileItemReaderBuilder<Equipe>()
                .name("equipeReader")
                .resource(new FileSystemResource(pathCnes + "/tbEquipe" + downloadProperties.getCnesCompetence() + ".csv"))
                .delimited()
                .delimiter(";")
                .quoteCharacter('"')
                .names("CO_MUNICIPIO","CO_AREA","SEQ_EQUIPE","CO_UNIDADE","TP_EQUIPE","CO_SUB_TIPO_EQUIPE","NO_REFERENCIA","DT_ATIVACAO","DT_DESATIVACAO","TP_POP_ASSIST_QUILOMB","TP_POP_ASSIST_ASSENT","TP_POP_ASSIST_GERAL","TP_POP_ASSIST_ESCOLA","TP_POP_ASSIST_PRONASCI","TP_POP_ASSIST_INDIGENA","TP_POP_ASSIST_RIBEIRINHA","TP_POP_ASSIST_SITUACAO_RUA","TP_POP_ASSIST_PRIV_LIBERDADE","TP_POP_ASSIST_CONFLITO_LEI","TP_POP_ASSIST_ADOL_CONF_LEI","CO_CNES_UOM","NU_CH_AMB_UOM","CD_MOTIVO_DESATIV","CD_TP_DESATIV","CO_PROF_SUS_PRECEPTOR","CO_CNES_PRECEPTOR","CO_EQUIPE","TO_CHAR(DT_ATUALIZACAO,'DD/MM/YYYY')","NO_USUARIO","TO_CHAR(DT_ATUALIZACAO_ORIGEM,'DD/MM/YYYY')")
                .fieldSetMapper(fieldSet -> new Equipe(
                        fieldSet.readInt("TP_EQUIPE"),
                        fieldSet.readString("NO_REFERENCIA"),
                        fieldSet.readString("CO_UNIDADE"),
                        fieldSet.readString("SEQ_EQUIPE")
                ))
                .linesToSkip(1)
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Equipe> equipeWriter(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Equipe>()
                .dataSource(dataSource)
                .sql("""
                    INSERT INTO equipe (
                        tipoEquipe,
                        noEquipe,
                        coUnidade,
                        ineEquipe
                    ) VALUES (
                        :tipoEquipe,
                        :noEquipe,
                        :coUnidade,
                        LPAD(:ineEquipe, 10, '0')
                    )
                    """)
                .beanMapped()
                .build();
    }

}
