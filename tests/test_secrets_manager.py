from __future__ import annotations

import pytest


@pytest.mark.parametrize("side", ["client", "server"])
def test_get_conn_value(
    side: str,
    secret_key,
    backend_path,
    default_conn_id,
    client_backend,  # noqa: ARG001 # init
):
    setup(is_server=side == "server")

    from airflow_fernet_secrets.backend import FernetLocalSecretsBackend

    backend = FernetLocalSecretsBackend(
        secret_key=secret_key, backend_file_path=backend_path
    )
    conn = backend.get_conn_value(default_conn_id)
    assert conn is not None


def setup(*, is_server: bool) -> None:
    import os

    from airflow_fernet_secrets.common.config import const
    from airflow_fernet_secrets.common.utils.reload import reload

    key = (const.CLIENT_ENV_PREFIX + const.ENV_IS_SERVER).upper()
    os.environ[key] = str(is_server)
    reload()

    from airflow_fernet_secrets.backend import FernetLocalSecretsBackend
    from airflow_fernet_secrets.common.config import IS_SERVER_FLAG

    assert IS_SERVER_FLAG is is_server
    assert (
        FernetLocalSecretsBackend.__module__.split(".")[-1] == "server"
        if is_server
        else "client"
    )
