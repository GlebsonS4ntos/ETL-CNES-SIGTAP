package com.Glebson.ETL.Domain.Sigtap;

public record CompatibilidadeProcedimentosSecundario(
        String codigoProcedimentoPrincipal,
        String codigoRegistroProcedimentoPrincipal,
        String codigoProcedimentoSecundario,
        String codigoRegistroProcedimentoSecundario,
        String compatibilidade,
        int qtdPermitidaProcedimentoSecundario,
        String competencia) {}
