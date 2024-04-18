# airflow-fernet-secrets

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-yellow.svg)](https://opensource.org/licenses/Apache-2.0)
[![github action](https://github.com/phi-friday/airflow-fernet-secrets/actions/workflows/check.yaml/badge.svg?event=push)](#)
[![PyPI version](https://badge.fury.io/py/airflow-fernet-secrets.svg)](https://badge.fury.io/py/airflow-fernet-secrets)
[![python version](https://img.shields.io/pypi/pyversions/airflow-fernet-secrets.svg)](#)

## how to install
```sh
pip install airflow-fernet-secrets
# or
# pip install airflow-fernet-secrets[asyncio]
```

## how to use
```toml
AIRFLOW__SECRETS__BACKEND=airflow.providers.fernet_secrets.secrets.secret_manager.FernetLocalSecretsBackend
# or
# AIRFLOW__SECRETS__BACKEND=airflow_fernet_secrets.secrets.server.ServerFernetLocalSecretsBackend
#
AIRFLOW__PROVIDERS_FERNET_SECRETS__SECRET_KEY=# some fernet key
# ex: 2eu7W6ULuYxnUB4Uz31IYddkdgboa5kLP24bYtegll0=
# or
# AIRFLOW__PROVIDERS_FERNET_SECRETS__SECRET_KEY_CMD=# some fernet key command
# ex: cat /dev/run/secret_key
# or
# AIRFLOW__PROVIDERS_FERNET_SECRETS__SECRET_KEY_SECRET=# some fernet key file
# ex: /dev/run/secret_key
AIRFLOW__PROVIDERS_FERNET_SECRETS__BACKEND_FILE=# some sqlite file path
# ex: /tmp/backend
# or
# AIRFLOW__PROVIDERS_FERNET_SECRETS__BACKEND_FILE_CMD=# some sqlite file path command
# ex: cat /tmp/where_is_backend
# or
# AIRFLOW__PROVIDERS_FERNET_SECRETS__BACKEND_FILE_SECRET=# some sqlite file path file
# ex: /tmp/where_is_backend
```

## TODO
- [x] exceptions
- [ ] mysql
- [x] mssql(`pymssql`)
- [ ] more tests