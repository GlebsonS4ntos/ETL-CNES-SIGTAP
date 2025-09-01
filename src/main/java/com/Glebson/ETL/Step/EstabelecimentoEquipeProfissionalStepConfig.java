package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Cnes.Equipe;
import com.Glebson.ETL.Domain.Cnes.EstabelecimentoEquipeProfissional;
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
public class EstabelecimentoEquipeProfissionalStepConfig {


    private final DownloadProperties downloadProperties;

    public EstabelecimentoEquipeProfissionalStepConfig(DownloadProperties downloadProperties) {
        this.downloadProperties = downloadProperties;
    }

    @Bean
    public Step estabelecimentoEquipeProfissionalStep(JobRepository repository, PlatformTransactionManager transactionManager,
                           FlatFileItemReader<EstabelecimentoEquipeProfissional> estabelecimentoEquipeProfissionalReader,
                           JdbcBatchItemWriter<EstabelecimentoEquipeProfissional> estabelecimentoEquipeProfissionalWriter,
                           TaskExecutor virtualTaskExecutor){
        return new StepBuilder("estabelecimentoEquipeProfissionalStep", repository)
                .<EstabelecimentoEquipeProfissional, EstabelecimentoEquipeProfissional>chunk(1000, transactionManager)
                .reader(estabelecimentoEquipeProfissionalReader)
                .writer(estabelecimentoEquipeProfissionalWriter)
                .taskExecutor(virtualTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EstabelecimentoEquipeProfissional> estabelecimentoEquipeProfissionalFlatFileItemReader(@Value("#{jobExecutionContext['pathCnes']}") String pathCnes){
        return new FlatFileItemReaderBuilder<EstabelecimentoEquipeProfissional>()
                .name("estabelecimentoEquipeProfissionalReader")
                .resource(new FileSystemResource(pathCnes + "/rlEstabEquipeProf" + downloadProperties.getCnesCompetence() + ".csv"))
                .delimited()
                .delimiter(";")
                .quoteCharacter('"')
                .names("CO_MUNICIPIO","CO_AREA","SEQ_EQUIPE","CO_PROFISSIONAL_SUS","CO_UNIDADE","CO_CBO","TP_SUS_NAO_SUS","IND_VINCULACAO","CO_MICROAREA","DT_ENTRADA","DT_DESLIGAMENTO","CO_CNES_OUTRAEQUIPE","CO_MUNICIPIO_OUTRAEQUIPE","CO_AREA_OUTRAEQUIPE","CO_PROFISSIONAL_SUS_COMPL","CO_CBO_CH_COMPL","ST_EQUIPEMINIMA","CO_MUN_ATUACAO","TO_CHAR(DT_ATUALIZACAO,'DD/MM/YYYY')","NO_USUARIO","TO_CHAR(DT_ATUALIZACAO_ORIGEM,'DD/MM/YYYY')")
                .fieldSetMapper(fieldSet -> new EstabelecimentoEquipeProfissional(
                        fieldSet.readString("SEQ_EQUIPE"),
                        fieldSet.readString("CO_PROFISSIONAL_SUS"),
                        fieldSet.readString("CO_CBO"),
                        fieldSet.readString("CO_UNIDADE")
                ))
                .linesToSkip(1)
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<EstabelecimentoEquipeProfissional> estabelecimentoEquipeProfissionalWriter(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<EstabelecimentoEquipeProfissional>()
                .dataSource(dataSource)
                .sql("""
                    INSERT INTO estabelecimentoEquipeProfissional (
                        ineEquipe,
                        coProfissional,
                        coCbo,
                        coUnidade
                    ) VALUES (
                        LPAD(:ineEquipe, 10, '0'),
                        :coProfissional,
                        :coCbo,
                        :coUnidade
                    )
                    """)
                .beanMapped()
                .build();
    }


}