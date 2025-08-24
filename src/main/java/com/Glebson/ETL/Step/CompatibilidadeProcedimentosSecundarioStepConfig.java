package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Sigtap.CompatibilidadeProcedimentosSecundario;
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
public class CompatibilidadeProcedimentosSecundarioStepConfig {

    @Bean
    public Step compatibilidadeProcedimentosSecundarioStep(JobRepository repository, PlatformTransactionManager transactionManager,
                                                           FlatFileItemReader<CompatibilidadeProcedimentosSecundario> compatibilidadeProcedimentosSecundarioFlatFileItemReader,
                                                           JdbcBatchItemWriter<CompatibilidadeProcedimentosSecundario> compatibilidadeProcedimentosSecundarioJdbcBatchItemWriter,
                                                           TaskExecutor virtualTaskExecutor){
        return new StepBuilder("compatibilidadeProcedimentosSecundarioStep", repository)
                .<CompatibilidadeProcedimentosSecundario, CompatibilidadeProcedimentosSecundario>chunk(1000, transactionManager)
                .reader(compatibilidadeProcedimentosSecundarioFlatFileItemReader)
                .writer(compatibilidadeProcedimentosSecundarioJdbcBatchItemWriter)
                .taskExecutor(virtualTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<CompatibilidadeProcedimentosSecundario> compatibilidadeProcedimentosSecundarioReader(@Value("#{jobExecutionContext['pathSigtap']}") String sigtapPath){
        return new FlatFileItemReaderBuilder<CompatibilidadeProcedimentosSecundario>()
                .name("compatibilidadeProcedimentosSecundarioReader")
                .resource(new FileSystemResource(sigtapPath + "/rl_procedimento_compativel.txt"))
                .lineTokenizer(lengthColumns())
                .fieldSetMapper(fieldSet -> new CompatibilidadeProcedimentosSecundario(
                        fieldSet.readString("codigoProcedimentoPrincipal"),
                        fieldSet.readString("codigoRegistroProcedimentoPrincipal"),
                        fieldSet.readString("codigoProcedimentoSecundario"),
                        fieldSet.readString("codigoRegistroProcedimentoSecundario"),
                        fieldSet.readString("compatibilidade"),
                        fieldSet.readInt("qtdPermitidaProcedimentoSecundario"),
                        fieldSet.readString("competencia")
                )).build();
    }

    private LineTokenizer lengthColumns() {
        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();

        tokenizer.setNames(
                "codigoProcedimentoPrincipal",
                "codigoRegistroProcedimentoPrincipal",
                "codigoProcedimentoSecundario",
                "codigoRegistroProcedimentoSecundario",
                "compatibilidade",
                "qtdPermitidaProcedimentoSecundario",
                "competencia"
        );

        tokenizer.setColumns(
                new Range(1, 10),
                new Range(11, 12),
                new Range(13, 22),
                new Range(23, 24),
                new Range(25,25),
                new Range(26, 29),
                new Range(30, 35)
        );

        return tokenizer;
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<CompatibilidadeProcedimentosSecundario> compatibilidadeProcedimentosSecundarioWriter(@Qualifier("dataSource") DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<CompatibilidadeProcedimentosSecundario>()
                .dataSource(dataSource)
                .sql("""
                INSERT INTO compatibilidade_procedimentos_secundario (
                     codigo_procedimento_principal,
                     codigo_registro_procedimento_principal,
                     codigo_procedimento_secundario,
                     codigo_registro_procedimento_secundario,
                     compatibilidade,
                     qtd_permitida_procedimento_secundario,
                     competencia
                ) VALUES (
                    :codigoProcedimentoPrincipal,
                    :codigoRegistroProcedimentoPrincipal,
                    :codigoProcedimentoSecundario,
                    :codigoRegistroProcedimentoSecundario,
                    :compatibilidade,
                    :qtdPermitidaProcedimentoSecundario,
                    :competencia
                )
                """)
                .beanMapped()
                .build();
    }
}
