"""Blocos comuns às DAGs: argumentos padrão e tasks que executam containers."""

from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from bigdata_pipeline import alerts, config


def default_args() -> dict:
    # O callback roda quando a task falha de vez, depois das novas tentativas.
    return {
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alerts.failure_callbacks(),
    }


def container_task(task_id: str, image: str, command: list[str], environment: dict) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=environment,
        docker_url=config.DOCKER_URL,
        network_mode=config.DOCKER_NETWORK,
        auto_remove="force",
        # Sem bind de /tmp do host: não funciona com o socket via proxy/Docker Desktop.
        mount_tmp_dir=False,
    )
