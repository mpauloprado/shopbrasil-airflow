# Testes do DAG. Rodar com o ambiente no ar:
#   docker compose exec airflow-scheduler python -m pytest -q /opt/airflow/tests

from airflow.models import DagBag

from shopbrasil.operators import ValidarProdutosOperator


def carregar_dag():
    bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("shopbrasil_precos_categoria")


def test_agendamento():
    dag = carregar_dag()
    assert dag.schedule_interval == "0 6 * * *"
    assert dag.timezone.name == "America/Sao_Paulo"
    assert dag.catchup is False


def test_buscar_produtos_tem_retry_e_callbacks():
    task = carregar_dag().get_task("ingestao.buscar_produtos")
    assert task.retries == 4
    assert task.retry_exponential_backoff
    assert task.on_failure_callback and task.on_retry_callback and task.on_success_callback


def test_metricas_usam_o_pool():
    task = carregar_dag().get_task("analise.calcular_metricas")
    assert task.pool == "ecommerce_pool"


def test_operador_descarta_produto_invalido():
    op = ValidarProdutosOperator(
        task_id="teste",
        produtos=[
            {"id": 1, "title": "Camiseta", "price": 10.0, "category": "roupas"},
            {"id": 2, "title": "Bermuda", "price": -5, "category": "roupas"},
        ],
    )
    validos = op.execute({})
    assert [p["id"] for p in validos] == [1]
