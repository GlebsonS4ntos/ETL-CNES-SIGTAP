package com.Glebson.ETL.Domain.Sigtap;

public record Procedimento(
        String coProcedimento,
        String noProcedimento,
        String tpComplexidade,
        String tpSexo,
        Integer qtMaximaExecucao,
        Integer qtDiasPermanencia,
        Integer qtPontos,
        Integer vlIdadeMinima,
        Integer vlIdadeMaxima,
        Integer vlSh,
        Integer vlSa,
        Integer vlSp,
        String coFinanciamento,
        String coRubrica,
        Integer qtTempoPermanencia,
        String dtCompetencia
) {}