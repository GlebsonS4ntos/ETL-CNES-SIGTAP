package com.Glebson.ETL.Step;

import com.Glebson.ETL.Domain.Sigtap.Procedimento;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.batch.item.file.transform.Range;

import javax.sql.DataSource;

@Configuration
public class ProcedimentoStepConfig {

    @Bean
    @StepScope
    public FlatFileItemReader<Procedimento> procedimentosReader(@Value("#{jobExecutionContext['pathSigtap']}") String sigtapPath){
        return new FlatFileItemReaderBuilder<Procedimento>()
                .name("procedimentoReader")
                .resource(new FileSystemResource(sigtapPath + "/tb_procedimento.txt"))
                .lineTokenizer(lengthColumns())
                .fieldSetMapper(fieldSet -> new Procedimento(
                    fieldSet.readString("coProcedimento"),
                    fieldSet.readString("noProcedimento").trim(),
                    fieldSet.readString("tpComplexidade"),
                    fieldSet.readString("tpSexo"),
                    fieldSet.readInt("qtMaximaExecucao"),
                    fieldSet.readInt("qtDiasPermanencia"),
                    fieldSet.readInt("qtPontos"),
                    fieldSet.readInt("vlIdadeMinima"),
                    fieldSet.readInt("vlIdadeMaxima"),
                    fieldSet.readInt("vlSh"),
                    fieldSet.readInt("vlSa"),
                    fieldSet.readInt("vlSp"),
                    fieldSet.readString("coFinanciamento"),
                    fieldSet.readString("coRubrica"),
                    fieldSet.readInt("qtTempoPermanencia"),
                    fieldSet.readString("dtCompetencia") // pode depois converter p/ YearMonth se quiser
                )).build();
    }

    private LineTokenizer lengthColumns() {
        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();

        tokenizer.setNames(
                "coProcedimento",
                "noProcedimento",
                "tpComplexidade",
                "tpSexo",
                "qtMaximaExecucao",
                "qtDiasPermanencia",
                "qtPontos",
                "vlIdadeMinima",
                "vlIdadeMaxima",
                "vlSh",
                "vlSa",
                "vlSp",
                "coFinanciamento",
                "coRubrica",
                "qtTempoPermanencia",
                "dtCompetencia"
        );

        tokenizer.setColumns(
                new Range(1, 10),
                new Range(11, 260),
                new Range(261, 261),
                new Range(262, 262),
                new Range(263, 266),
                new Range(267, 270),
                new Range(271, 274),
                new Range(275, 278),
                new Range(279, 282),
                new Range(283, 294),
                new Range(295, 306),
                new Range(307, 318),
                new Range(319, 320),
                new Range(321, 326),
                new Range(327, 330),
                new Range(331, 336)
        );

        return tokenizer;
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Procedimento> ProcedimentoWriter(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Procedimento>()
                .dataSource(dataSource)
                .sql("""
                INSERT INTO procedimento (
                    codigo,
                    nome,
                    tp_complexidade,
                    tp_sexo,
                    qt_maxima_execucao,
                    qt_dias_permanencia,
                    qt_pontos,
                    vl_idade_minima,
                    vl_idade_maxima,
                    vl_sh,
                    vl_sa,
                    vl_sp,
                    co_financiamento,
                    co_rubrica,
                    qt_tempo_permanencia,
                    dt_competencia
                ) VALUES (
                    :coProcedimento,
                    :noProcedimento,
                    :tpComplexidade,
                    :tpSexo,
                    :qtMaximaExecucao,
                    :qtDiasPermanencia,
                    :qtPontos,
                    :vlIdadeMinima,
                    :vlIdadeMaxima,
                    :vlSh,
                    :vlSa,
                    :vlSp,
                    :coFinanciamento,
                    :coRubrica,
                    :qtTempoPermanencia,
                    :dtCompetencia
                )
                """)
                .beanMapped()
                .build();
    }

}
