DROP TABLE IF EXISTS procedimento;
DROP TABLE IF EXISTS tipo_procedimento;
DROP TABLE IF EXISTS compatibilidade_procedimentos_secundario;
DROP TABLE IF EXISTS compatibilidade_procedimentos_cbo;
DROP TABLE IF EXISTS municipio;
DROP TABLE IF EXISTS estado;
DROP TABLE IF EXISTS cbo;
DROP TABLE IF EXISTS profissional;
DROP TABLE IF EXISTS estabelecimento;
DROP TABLE IF EXISTS cargaHorariaCboProfissional;
DROP TABLE IF EXISTS equipe;
DROP TABLE IF EXISTS estabelecimentoEquipeProfissional;

CREATE TABLE procedimento (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(10) NOT NULL,
    nome VARCHAR(256) NOT NULL,
    tp_complexidade CHAR(1),
    tp_sexo CHAR(1),
    qt_maxima_execucao INT,
    qt_dias_permanencia INT,
    qt_pontos INT,
    vl_idade_minima INT,
    vl_idade_maxima INT,
    vl_sh INT,
    vl_sa INT,
    vl_sp INT,
    co_financiamento VARCHAR(2),
    co_rubrica VARCHAR(6),
    qt_tempo_permanencia INT,
    dt_competencia CHAR(6)
);

CREATE TABLE compatibilidade_procedimentos_cbo (
    id SERIAL PRIMARY KEY,
    codigo_procedimento VARCHAR(10) NOT NULL,
    codigo_cbo VARCHAR(10) NOT NULL,
    competencia CHAR(6) NOT NULL
);

CREATE TABLE compatibilidade_procedimentos_secundario (
    id SERIAL PRIMARY KEY,
    codigo_procedimento_principal VARCHAR(10) NOT NULL,
    codigo_registro_procedimento_principal VARCHAR(2) NOT NULL,
    codigo_procedimento_secundario VARCHAR(10) NOT NULL,
    codigo_registro_procedimento_secundario VARCHAR(2) NOT NULL,
    compatibilidade CHAR(6) NOT NULL,
    qtd_permitida_procedimento_secundario INT NOT NULL,
    competencia CHAR(6) NOT NULL
);

CREATE TABLE tipo_procedimento (
    id SERIAL PRIMARY KEY,
    codigo_tipo VARCHAR(5) NOT NULL,
    nome_tipo VARCHAR(256) NOT NULL,
    competencia CHAR(6) NOT NULL
);

CREATE TABLE estado (
    id SERIAL PRIMARY KEY,
    coUf VARCHAR(10) NOT NULL,
    coSigla VARCHAR(10) NOT NULL,
    noEstado VARCHAR(256) NOT NULL
);

CREATE TABLE municipio (
    id SERIAL PRIMARY KEY,
    coMunicipio VARCHAR(10) NOT NULL,
    noMunicipio VARCHAR(256) NOT NULL,
    coSigla VARCHAR(10) NOT NULL
);

CREATE TABLE cbo (
    id SERIAL PRIMARY KEY,
    coCbo VARCHAR(10) NOT NULL,
    noAtividadeProfissional VARCHAR(256) NOT NULL
);

CREATE TABLE profissional (
    id SERIAL PRIMARY KEY,
    coProfissional VARCHAR(256) NOT NULL,
    noProfissional VARCHAR(256) NOT NULL,
    cns VARCHAR(15) NOT NULL
);

CREATE TABLE estabelecimento (
    id SERIAL PRIMARY KEY,
    coEstabelecimento VARCHAR(256) NOT NULL,
    cnes VARCHAR(20) NOT NULL,
    noEstabelecimento VARCHAR(256) NOT NULL,
    coMunicipio VARCHAR(10) NOT NULL,
    coEstado VARCHAR(10) NOT NULL
);

CREATE TABLE cargaHorariaCboProfissional (
    id SERIAL PRIMARY KEY,
    coUnidade VARCHAR(256) NOT NULL,
    coProfissional VARCHAR(256) NOT NULL,
    coCbo VARCHAR(10) NOT NULL,
    qtdCargaHoraria VARCHAR(10)
);

CREATE TABLE equipe (
    id SERIAL PRIMARY KEY,
    tipoEquipe INT NOT NULL,
    noEquipe VARCHAR(256) NOT NULL,
    coUnidade VARCHAR(256) NOT NULL,
    ineEquipe VARCHAR(10) NOT NULL
);

CREATE TABLE estabelecimentoEquipeProfissional (
    id SERIAL PRIMARY KEY,
    ineEquipe VARCHAR(10) NOT NULL,
    coProfissional VARCHAR(256) NOT NULL,
    coCbo VARCHAR(256) NOT NULL,
    coUnidade VARCHAR(256) NOT NULL
);