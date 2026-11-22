# ShopBrasil - preços por categoria com Airflow

Atividade 01 de Orquestração de Workflows.

O DAG roda todo dia às 06:00 (horário de Brasília), busca os produtos da FakeStore API
(https://fakestoreapi.com), calcula preço médio, mínimo, máximo e quantidade de produtos por
categoria e grava no PostgreSQL.

## Como rodar

Precisa do Docker com Docker Compose.

```bash
git clone https://github.com/mpauloprado/shopbrasil-airflow.git
cd shopbrasil-airflow
docker compose up -d
```

Na primeira vez demora alguns minutos para baixar as imagens. Depois é só abrir
http://localhost:8080 e entrar com admin / admin.

O `airflow-init` cria o usuário, a Connection `postgres_shopbrasil` e o pool `ecommerce_pool`
com 2 slots. As tabelas são criadas pelo `sql/init.sql`. O DAG já sobe ativo; para rodar na hora
é só usar o Trigger.

Ver o que foi gravado:

```bash
docker compose exec postgres-analitico psql -U shopbrasil -d analytics -c "SELECT * FROM metricas_categoria;"
docker compose exec postgres-analitico psql -U shopbrasil -d analytics -c "SELECT * FROM historico_metricas_categoria;"
```

Rodar os testes:

```bash
docker compose exec airflow-scheduler python -m pytest -q -p no:cacheprovider /opt/airflow/tests
```

Para parar: `docker compose down` (ou `docker compose down -v` para apagar os dados).
Se a porta 8080 estiver ocupada, troque para `"8081:8080"` no `docker-compose.yml`.

## O DAG

Feito com TaskFlow (`@dag` / `@task`), agendado com `0 6 * * *`, `start_date` em
America/Sao_Paulo (pendulum) e `catchup=False`. Os dados passam de uma task para outra pelo
`return` (XCom), só com os campos necessários.

```
buscar_produtos -> validar_produtos -> agrupar_por_categoria      (ingestao)
calcular_metricas, uma task por categoria -> consolidar_metricas  (analise)
salvar_snapshot -> salvar_historico                               (persistencia)
```

A ingestão é linear, o `calcular_metricas` faz o fan-out e o `consolidar_metricas` o fan-in.

- `buscar_produtos`: chama a API. Se der erro, mostra no log e faz `raise` para o Airflow
  tentar de novo. Tem 4 retries com exponential backoff e os callbacks de sucesso, retry e falha.
- `validar_produtos`: operador próprio (`ValidarProdutosOperator`, em `plugins/shopbrasil`).
  Descarta produto sem id, título ou categoria, ou com preço inválido.
- `calcular_metricas`: usa `.expand()` (Dynamic Task Mapping). Categoria nova na API vira task
  nova sem mexer no código. Usa o pool `ecommerce_pool`, então roda no máximo 2 ao mesmo tempo.
- `salvar_snapshot` e `salvar_historico`: gravam com `PostgresHook` usando a Connection.

## Idempotência

A tabela `metricas_categoria` tem chave `(data_referencia, categoria)` e o insert usa
`ON CONFLICT DO UPDATE`. Rodar o mesmo dia de novo atualiza as linhas em vez de duplicar.

O histórico (`historico_metricas_categoria`) é append, com a data da execução, mas tem
`UNIQUE (dag_run_id, categoria)`, então repetir a mesma run não duplica.

Para conferir, dê Clear numa run, rode de novo e veja que a contagem continua igual.

## Simulando a API fora do ar

```bash
docker compose exec airflow-scheduler airflow variables set fakestore_url http://api-fora-do-ar.local
```

Rodando o DAG, a `buscar_produtos` entra em retry e, quando acabam as tentativas, o
`on_failure_callback` escreve `[ALERTA SIMULADO]` no log. Para voltar ao normal:

```bash
docker compose exec airflow-scheduler airflow variables delete fakestore_url
```

Versões: Airflow 2.9.3 (Python 3.11), provider Postgres 5.11.0 e PostgreSQL 16.
