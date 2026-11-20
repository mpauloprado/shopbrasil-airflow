from airflow.models.baseoperator import BaseOperator


class ValidarProdutosOperator(BaseOperator):
    """
    Confere o schema dos produtos que vieram da API.
    Cada produto precisa ter id inteiro, title e category preenchidos e price >= 0.
    Produtos com problema são descartados. Se não sobrar nenhum, a task falha.
    """

    # permite receber o retorno (XCom) da task buscar_produtos
    template_fields = ("produtos",)

    def __init__(self, produtos, **kwargs):
        super().__init__(**kwargs)
        self.produtos = produtos

    def execute(self, context):
        validos = []
        for p in self.produtos:
            if produto_valido(p):
                validos.append(p)
            else:
                self.log.warning("Produto descartado: %s", p)

        self.log.info("%d de %d produtos válidos", len(validos), len(self.produtos))
        if not validos:
            raise ValueError("Nenhum produto válido recebido da API")
        return validos


def produto_valido(p):
    return (
        isinstance(p.get("id"), int)
        and isinstance(p.get("title"), str) and p["title"] != ""
        and isinstance(p.get("price"), (int, float)) and p["price"] >= 0
        and isinstance(p.get("category"), str) and p["category"] != ""
    )
