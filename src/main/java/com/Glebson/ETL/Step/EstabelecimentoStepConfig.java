package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Cnes.Estabelecimento;
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
public class EstabelecimentoStepConfig {
    private final DownloadProperties downloadProperties;

    public EstabelecimentoStepConfig(DownloadProperties downloadProperties) {
        this.downloadProperties = downloadProperties;
    }

    @Bean
    public Step estabelecimentoStep(JobRepository repository, PlatformTransactionManager transactionManager,
                                 FlatFileItemReader<Estabelecimento> estabelecimentoReader,
                                 JdbcBatchItemWriter<Estabelecimento> estabelecimentoWriter,
                                 TaskExecutor virtualTaskExecutor){
        return new StepBuilder("estabelecimentoStep", repository)
                .<Estabelecimento, Estabelecimento>chunk(1000, transactionManager)
                .reader(estabelecimentoReader)
                .writer(estabelecimentoWriter)
                .taskExecutor(virtualTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Estabelecimento> estabelecimentoFlatFileItemReader(@Value("#{jobExecutionContext['pathCnes']}") String pathCnes){
        return new FlatFileItemReaderBuilder<Estabelecimento>()
                .name("estabelecimentoReader")
                .resource(new FileSystemResource(pathCnes + "/tbEstabelecimento" + downloadProperties.getCnesCompetence() + ".csv"))
                .delimited()
                .delimiter(";")
                .quoteCharacter('"')
                .names("CO_UNIDADE","CO_CNES","NU_CNPJ_MANTENEDORA","TP_PFPJ","NIVEL_DEP","NO_RAZAO_SOCIAL","NO_FANTASIA","NO_LOGRADOURO","NU_ENDERECO","NO_COMPLEMENTO","NO_BAIRRO","CO_CEP","CO_REGIAO_SAUDE","CO_MICRO_REGIAO","CO_DISTRITO_SANITARIO","CO_DISTRITO_ADMINISTRATIVO","NU_TELEFONE","NU_FAX","NO_EMAIL","NU_CPF","NU_CNPJ","CO_ATIVIDADE","CO_CLIENTELA","NU_ALVARA","DT_EXPEDICAO","TP_ORGAO_EXPEDIDOR","DT_VAL_LIC_SANI","TP_LIC_SANI","TP_UNIDADE","CO_TURNO_ATENDIMENTO","CO_ESTADO_GESTOR","CO_MUNICIPIO_GESTOR","TO_CHAR(DT_ATUALIZACAO,'DD/MM/YYYY')","CO_USUARIO","CO_CPFDIRETORCLN","REG_DIRETORCLN","ST_ADESAO_FILANTROP","CO_MOTIVO_DESAB","NO_URL","NU_LATITUDE","NU_LONGITUDE","TO_CHAR(DT_ATU_GEO,'DD/MM/YYYY')","NO_USUARIO_GEO","CO_NATUREZA_JUR","TP_ESTAB_SEMPRE_ABERTO","ST_GERACREDITO_GERENTE_SGIF","ST_CONEXAO_INTERNET","CO_TIPO_UNIDADE","NO_FANTASIA_ABREV","TP_GESTAO","TO_CHAR(DT_ATUALIZACAO_ORIGEM,'DD/MM/YYYY')","CO_TIPO_ESTABELECIMENTO","CO_ATIVIDADE_PRINCIPAL","ST_CONTRATO_FORMALIZADO","CO_TIPO_ABRANGENCIA","ST_COWORKING")
                .fieldSetMapper(fieldSet -> new Estabelecimento(
                        fieldSet.readString("CO_UNIDADE"),
                        fieldSet.readString("CO_CNES"),
                        fieldSet.readString("NO_FANTASIA").trim(),
                        fieldSet.readString("CO_MUNICIPIO_GESTOR"),
                        fieldSet.readString("CO_ESTADO_GESTOR")
                ))
                .linesToSkip(1)
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Estabelecimento> estabelecimentoWriter(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Estabelecimento>()
                .dataSource(dataSource)
                .sql("""
                    INSERT INTO estabelecimento (
                        coEstabelecimento,
                        cnes,
                        noEstabelecimento,
                        coMunicipio,
                        coEstado
                    ) VALUES (
                        :coEstabelecimento,
                        :cnes,
                        :noEstabelecimento,
                        :coMunicipio,
                        :coEstado
                    )
                    """)
                .beanMapped()
                .build();
    }
}
