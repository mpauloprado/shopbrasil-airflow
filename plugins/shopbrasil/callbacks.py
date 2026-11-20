# Callbacks usados no DAG.
# O alerta é simulado: só escreve no log da task. Num ambiente real
# aqui entraria o envio para Slack ou e-mail.


def avisar_sucesso(context):
    task_id = context["task"].task_id
    print(f"Task {task_id} terminou com sucesso")


def avisar_retry(context):
    task_id = context["task"].task_id
    # quando o retry é chamado, o try_number já aponta para a próxima tentativa
    tentativa = context["task_instance"].try_number - 1
    print(f"Task {task_id} falhou na tentativa {tentativa}, vai tentar de novo. Erro: {context['exception']}")


def avisar_falha(context):
    task_id = context["task"].task_id
    run_id = context["run_id"]
    print(f"[ALERTA SIMULADO] Task {task_id} falhou na run {run_id}. Erro: {context['exception']}")
