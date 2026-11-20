"""
Atividade 01 - Pipeline de preços da ShopBrasil

Todo dia às 06:00 (horário de Brasília) busca os produtos da FakeStore API,
calcula preço médio, mínimo, máximo e quantidade de produtos por categoria
e grava o resultado no PostgreSQL.

Fluxo:
  ingestao:     buscar_produtos -> validar_produtos -> agrupar_por_categoria   (linear)
  analise:      calcular_metricas, uma task por categoria                      (fan-out)
                consolidar_metricas                                            (fan-in)
  persistencia: salvar_snapshot -> salvar_historico
"""
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from shopbrasil.callbacks import avisar_falha, avisar_retry, avisar_sucesso
from shopbrasil.operators import ValidarProdutosOperator

CONEXAO = "postgres_shopbrasil"


def dia_da_execucao():
    # Dia (em São Paulo) a que a execução se refere.
    # Se a mesma run for rodada de novo, o dia é o mesmo, e o UPSERT só atualiza a linha.
    context = get_current_context()
    return context["data_interval_end"].in_timezone("America/Sao_Paulo").to_date_string()


@dag(
    dag_id="shopbrasil_precos_categoria",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={"on_failure_callback": avisar_falha},
    tags=["shopbrasil", "atividade01"],
    doc_md=__doc__,
)
def shopbrasil_precos_categoria():

    with TaskGroup("ingestao"):

        @task(
            retries=4,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=True,  # espera cresce a cada tentativa
            max_retry_delay=timedelta(minutes=10),
            on_failure_callback=avisar_falha,
            on_retry_callback=avisar_retry,
            on_success_callback=avisar_sucesso,
        )
        def buscar_produtos():
            # A URL fica numa Variable só para dar para simular a API fora do ar
            url = Variable.get("fakestore_url", default_var="https://fakestoreapi.com") + "/products"
            try:
                resposta = requests.get(url, timeout=30)
                resposta.raise_for_status()
                produtos = resposta.json()
            except Exception as erro:
                print(f"Erro ao buscar produtos em {url}: {erro}")
                raise  # relança o erro para o Airflow fazer o retry

            print(f"{len(produtos)} produtos recebidos da API")
            # Só os campos que vamos usar vão para o XCom
            return [
                {"id": p.get("id"), "title": p.get("title"), "price": p.get("price"), "category": p.get("category")}
                for p in produtos
            ]

        produtos = buscar_produtos()

        validar = ValidarProdutosOperator(task_id="validar_produtos", produtos=produtos)

        @task
        def agrupar_por_categoria(produtos):
            grupos = {}
            for p in produtos:
                grupos.setdefault(p["category"], []).append(p["price"])
            print("Categorias encontradas:", list(grupos))
            # Uma entrada por categoria. Categoria nova vira task nova, sem mexer no código.
            return [{"categoria": nome, "precos": precos} for nome, precos in grupos.items()]

        categorias = agrupar_por_categoria(validar.output)

    with TaskGroup("analise"):

        @task(pool="ecommerce_pool")
        def calcular_metricas(categoria):
            precos = categoria["precos"]
            return {
                "categoria": categoria["categoria"],
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_minimo": min(precos),
                "preco_maximo": max(precos),
                "qtd_produtos": len(precos),
            }

        @task
        def consolidar_metricas(metricas):
            metricas = list(metricas)
            for m in metricas:
                print(m)
            return metricas

        metricas = calcular_metricas.expand(categoria=categorias)
        consolidado = consolidar_metricas(metricas)

    with TaskGroup("persistencia"):

        @task
        def salvar_snapshot(metricas):
            # UPSERT: se já existe linha para o dia e a categoria, atualiza em vez de duplicar
            dia = dia_da_execucao()
            run_id = get_current_context()["run_id"]

            hook = PostgresHook(postgres_conn_id=CONEXAO)
            conn = hook.get_conn()
            cursor = conn.cursor()
            for m in metricas:
                cursor.execute(
                    """
                    INSERT INTO metricas_categoria
                        (data_referencia, categoria, preco_medio, preco_minimo, preco_maximo, qtd_produtos, dag_run_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (data_referencia, categoria) DO UPDATE SET
                        preco_medio = EXCLUDED.preco_medio,
                        preco_minimo = EXCLUDED.preco_minimo,
                        preco_maximo = EXCLUDED.preco_maximo,
                        qtd_produtos = EXCLUDED.qtd_produtos,
                        dag_run_id = EXCLUDED.dag_run_id,
                        atualizado_em = NOW()
                    """,
                    (dia, m["categoria"], m["preco_medio"], m["preco_minimo"], m["preco_maximo"],
                     m["qtd_produtos"], run_id),
                )
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Snapshot de {dia}: {len(metricas)} categorias gravadas")

        @task
        def salvar_historico(metricas):
            # Append: cada execução acrescenta suas linhas, para acompanhar a evolução dos preços.
            # Se a mesma run for repetida, o ON CONFLICT ignora as linhas que já existem.
            dia = dia_da_execucao()
            run_id = get_current_context()["run_id"]
            agora = pendulum.now("America/Sao_Paulo")

            hook = PostgresHook(postgres_conn_id=CONEXAO)
            conn = hook.get_conn()
            cursor = conn.cursor()
            for m in metricas:
                cursor.execute(
                    """
                    INSERT INTO historico_metricas_categoria
                        (data_execucao, data_referencia, categoria, preco_medio, preco_minimo, preco_maximo,
                         qtd_produtos, dag_run_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (dag_run_id, categoria) DO NOTHING
                    """,
                    (agora, dia, m["categoria"], m["preco_medio"], m["preco_minimo"], m["preco_maximo"],
                     m["qtd_produtos"], run_id),
                )
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Histórico de {dia} gravado")

        snapshot = salvar_snapshot(consolidado)
        historico = salvar_historico(consolidado)
        snapshot >> historico


shopbrasil_precos_categoria()
