# ETL-CNES-SIGTAP

Projeto em Java (Spring Batch) para processar e carregar dados dos sistemas **CNES** e **SIGTAP** em PostgreSQL.

## 📌 Visão Geral

Este ETL executa um fluxo de processamento em lote, com múltiplos steps executados **em paralelo em nível de fluxo** e **paralelismo interno em cada step** com threads virtuais.

Foi projetado para processar **milhões de registros** de forma eficiente e escalável.

## ⚡ Funcionalidades

- Processamento paralelo de **12 flows independentes** (procedimento, tipoProcedimento, compatibilidade, estado, município, CBO, profissional, estabelecimento, etc.), limitado a **2 threads físicas** por fluxo principal.
- Cada step chunk-oriented utiliza **threads virtuais** para melhorar throughput em operações I/O.
- Manejo de arquivos grandes (milhões de registros) com **alto desempenho**.
- Registro detalhado de logs em arquivo (`logs/etl-log.txt`).

## 🛠️ Tecnologias

- **Java 21**
- **Spring Boot 3.x**
- **Spring Batch 5.x**
- **PostgreSQL**
- **Threads virtuais**
- **Maven**

## ⚙️ Configuração

Antes de rodar, edite o arquivo `application.yml` conforme sua necessidade:

- **Banco de dados**
    - `spring.datasource.url` → host, porta e nome do banco
    - `spring.datasource.username` e `spring.datasource.password` → credenciais reais

- **Variáveis de ambiente obrigatórias**
  ```bash
  export CNES_URL=https://exemplo.gov.br/cnes.zip
  export SIGTAP_URL=https://exemplo.gov.br/sigtap.zip
  export CNES_COMPETENCE=202508
