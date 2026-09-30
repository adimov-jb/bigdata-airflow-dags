"""Alerta de falha por e-mail, compartilhado pelas DAGs."""

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from bigdata_pipeline import config

SUBJECT = "[bigdata] Falha em {{ ti.dag_id }}.{{ ti.task_id }} ({{ ds }})"

HTML = """
<p>A task <b>{{ ti.task_id }}</b> da DAG <b>{{ ti.dag_id }}</b> falhou depois de
{{ ti.try_number }} tentativa(s).</p>
<ul>
  <li>Run: {{ run_id }}</li>
  <li>Data lógica: {{ ds }}</li>
  <li>Erro: {{ exception }}</li>
</ul>
<p><a href="{{ ti.log_url }}">Abrir o log da task</a></p>
"""


def failure_callbacks() -> list:
    """Callbacks de falha das tasks. Lista vazia quando não há destinatários (ambiente local)."""
    if not config.ALERT_EMAILS:
        return []
    return [SmtpNotifier(to=list(config.ALERT_EMAILS), subject=SUBJECT, html_content=HTML)]
