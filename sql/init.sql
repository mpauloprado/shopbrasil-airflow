-- Tabelas da base analítica. Roda sozinho na primeira vez que o postgres-analitico sobe.

-- Snapshot do dia: uma linha por dia e categoria (é o que o painel lê).
-- A chave primária é o que garante que reprocessar o dia não duplica dados.
CREATE TABLE IF NOT EXISTS metricas_categoria (
    data_referencia DATE          NOT NULL,
    categoria       VARCHAR(100)  NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_minimo    NUMERIC(12,2) NOT NULL,
    preco_maximo    NUMERIC(12,2) NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    dag_run_id      VARCHAR(250)  NOT NULL,
    atualizado_em   TIMESTAMP     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (data_referencia, categoria)
);

-- Histórico em modo append: cada execução acrescenta suas linhas com a data da execução.
-- O UNIQUE evita linhas repetidas quando a mesma run é rodada de novo.
CREATE TABLE IF NOT EXISTS historico_metricas_categoria (
    id              SERIAL        PRIMARY KEY,
    data_execucao   TIMESTAMP     NOT NULL,
    data_referencia DATE          NOT NULL,
    categoria       VARCHAR(100)  NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_minimo    NUMERIC(12,2) NOT NULL,
    preco_maximo    NUMERIC(12,2) NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    dag_run_id      VARCHAR(250)  NOT NULL,
    UNIQUE (dag_run_id, categoria)
);
