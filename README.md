# ShopBrasil - Pipeline de preços por categoria no Airflow

Atividade 01 de Orquestração de Workflows.

O pipeline substitui o script que rodava no cron. Todo dia às 06:00 (horário de Brasília) ele
busca os produtos da [FakeStore API](https://fakestoreapi.com/docs), calcula o preço médio,
mínimo e máximo e a quantidade de produtos de cada categoria e grava o resultado no PostgreSQL.

## Como rodar

Precisa só do Docker com o Docker Compose (Docker Desktop no Windows/Mac).

```bash
git clone https://github.com/mpauloprado/shopbrasil-airflow.git
cd shopbrasil-airflow
docker compose up -d
```

Depois de uns 3 minutos (na primeira vez ele baixa as imagens e instala o provider do Postgres),
acesse **http://localhost:8080** com usuário **admin** e senha **admin**.

O `airflow-init` já cria tudo que o DAG precisa: o usuário, a Connection `postgres_shopbrasil`
e o Pool `ecommerce_pool` com 2 slots. As tabelas são criadas pelo `sql/init.sql`.

O DAG `shopbrasil_precos_categoria` já sobe ligado. Para não esperar as 06:00, é só clicar em
**Trigger DAG** (botão ▶).

Para ver o que foi gravado:

```bash
docker compose exec postgres-analitico psql -U shopbrasil -d analytics -c "SELECT * FROM metricas_categoria;"
docker compose exec postgres-analitico psql -U shopbrasil -d analytics -c "SELECT * FROM historico_metricas_categoria;"
```

Para rodar os testes:

```bash
docker compose exec airflow-scheduler python -m pytest -q -p no:cacheprovider /opt/airflow/tests
```

Para desligar: `docker compose down` (mantém os dados) ou `docker compose down -v` (apaga tudo).

Se a porta 8080 já estiver em uso na máquina, troque `"8080:8080"` por `"8081:8080"` no
`docker-compose.yml` e acesse http://localhost:8081.

## Como o pipeline funciona

```
ingestao:      buscar_produtos -> validar_produtos -> agrupar_por_categoria     (linear)
analise:       calcular_metricas [uma task por categoria]                       (fan-out)
               consolidar_metricas                                              (fan-in)
persistencia:  salvar_snapshot -> salvar_historico
```

- **buscar_produtos** chama a API. Se der erro, registra no log e relança a exceção
  (`try/except` + `raise`) para o Airflow tentar de novo: são 4 retries com exponential backoff
  (30s, 60s, 120s...). Ela tem os três callbacks: `on_success_callback`, `on_retry_callback` e
  `on_failure_callback`.
- **validar_produtos** é o operador customizado `ValidarProdutosOperator` (em
  `plugins/shopbrasil/operators.py`). Descarta produtos sem id, título, categoria ou com preço
  inválido.
- **agrupar_por_categoria** monta uma lista com uma entrada por categoria. Essa lista entra no
  `calcular_metricas.expand(...)` (Dynamic Task Mapping). Se aparecer uma categoria nova na API,
  aparece uma task nova sem mudar o código.
- **calcular_metricas** usa o pool `ecommerce_pool`, então no máximo 2 categorias são
  calculadas ao mesmo tempo.
- **consolidar_metricas** junta o resultado de todas as categorias.
- **salvar_snapshot** grava em `metricas_categoria` com `PostgresHook` e a Connection do Airflow.
- **salvar_historico** grava em `historico_metricas_categoria` em modo append, com a data da
  execução, para acompanhar a evolução dos preços.

As tasks estão em 3 TaskGroups (`ingestao`, `analise` e `persistencia`) e os dados passam entre
elas pelo `return` (XCom). Só vão para o XCom os campos usados no cálculo.

### Por que não duplica

A tabela `metricas_categoria` tem chave primária `(data_referencia, categoria)` e o insert usa
`ON CONFLICT ... DO UPDATE`. A `data_referencia` é o dia da execução (fim do intervalo da run, no
horário de São Paulo), então rodar de novo a mesma run só atualiza as mesmas linhas.

No histórico cada run nova acrescenta linhas, mas existe um `UNIQUE (dag_run_id, categoria)`:
se a mesma run for repetida, as linhas que já existem são ignoradas.

Para testar: rode o DAG, depois faça **Clear** na mesma run e rode de novo. A contagem não muda:

```bash
docker compose exec postgres-analitico psql -U shopbrasil -d analytics -c "SELECT data_referencia, COUNT(*) FROM metricas_categoria GROUP BY 1;"
```

### Alerta de falha

O `on_failure_callback` está em todas as tasks (via `default_args`) e simula o envio de um alerta
escrevendo `[ALERTA SIMULADO]` no log da task.

Para simular a API fora do ar, aponte a Variable `fakestore_url` para um endereço que não existe,
rode o DAG e veja os retries e o alerta no log de `buscar_produtos`:

```bash
docker compose exec airflow-scheduler airflow variables set fakestore_url http://api-fora-do-ar.local
docker compose exec airflow-scheduler airflow variables delete fakestore_url
```

O segundo comando volta para a API normal.

## Requisitos da atividade

Obrigatórios:

- [x] TaskFlow API (`@dag` / `@task`), dependências pela chamada das funções
- [x] XComs pelo `return`, só com dados pequenos
- [x] Topologias linear, fan-out e fan-in
- [x] Timezone America/Sao_Paulo com pendulum, `start_date` e `catchup=False`
- [x] Task "Buscar Produtos" com retry e exponential backoff
- [x] `try/except` + `raise`
- [x] `on_failure_callback`, `on_retry_callback` e `on_success_callback` na task crítica
- [x] Dynamic Task Mapping com `.expand(...)`
- [x] Pool `ecommerce_pool` com 2 slots
- [x] Pelo menos 2 TaskGroups
- [x] Gravação com `PostgresHook` e Connection do Airflow
- [x] Gravação idempotente

Opcionais:

- [x] `ValidarProdutosOperator(BaseOperator)`
- [x] Tabela de histórico em append com a data da execução
- [x] `on_failure_callback` simulando alerta

## Estrutura

```
docker-compose.yml                    Airflow + 2 PostgreSQL (metadados e analítico)
dags/shopbrasil_precos_categoria.py   o DAG
plugins/shopbrasil/operators.py       ValidarProdutosOperator
plugins/shopbrasil/callbacks.py       callbacks de sucesso, retry e falha
sql/init.sql                          tabelas da base analítica
tests/test_pipeline.py                testes do DAG e do operador
```

Versões: Airflow 2.9.3 (Python 3.11), provider Postgres 5.11.0 e PostgreSQL 16.
