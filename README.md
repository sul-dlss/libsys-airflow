[![Coverage Status](https://coveralls.io/repos/github/sul-dlss/libsys-airflow/badge.svg?branch=main)](https://coveralls.io/github/sul-dlss/libsys-airflow?branch=main)

# libsys-airflow

Airflow DAGS for libsys processes and migrating ILS data into FOLIO

## Dependency Management and Packaging

To install the dependencies, run:

- `pip install -r requirements.txt`
- `poetry install`
- `brew install libmagic`, `sudo apt-get install -y libmagic-dev`, or equivalent

### Adding new dependencies

When adding a new dependency to the application, follow the `poetry add` documentation (https://python-poetry.org/docs/cli/#add) to ensure that the dependency is captured in `pyproject.toml`.

## Running Locally with Docker

Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).

> **NOTE** Make sure there is enough RAM available locally for the
> docker daemon, we recommend at least 5GB.

1. Clone repository `git clone https://github.com/sul-dlss/libsys-airflow.git`
1. Start up docker locally.
1. Create a `.env` file with the `AIRFLOW_UID` and `AIRFLOW_GROUP` values. For local development these can usually be `AIRFLOW_UID=50000` and `AIRFLOW_GROUP=0`. (See [Airflow docs](https://airflow.apache.org/docs/apache-airflow/2.5.0/howto/docker-compose/index.html#setting-the-right-airflow-user) for more info.)
1. Add to the `.env` values for environment variables used by DAGs. (These are usually applied to VMs by puppet.)

- `AIRFLOW_VAR_OKAPI_URL`
- `AIRFLOW_VAR_FOLIO_URL`
- `AIRFLOW_VAR_FOLIO_USER`
- `AIRFLOW_VAR_FOLIO_PASSWORD`
- `AIRFLOW_KEYCLOAK_CLIENT_SECRET` (only needed when running against Keycloak — see [Authentication](#authentication) below)

  These environment variables must be prefixed with `AIRFLOW_VAR_` to be accessible to DAGs. (See [Airflow env var documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html#storing-variables-in-environment-variables and `docker-compose.yml`).) They can have placeholder values. The secrets are in vault, not prefixed by `AIRFLOW_VAR_`: `vault kv list puppet/application/libsys_airflow/{env}`.

  Example script to quickly populate your .env file for dev:
  ```
  for i in `vault kv list puppet/application/libsys_airflow/dev`; do val=$(echo $i| tr '[a-z]' '[A-Z]'); echo AIRFLOW_VAR_$val=`vault kv get -field=content puppet/application/libsys_airflow/dev/$i`; done
  ```

  **NOTE** In order to connect to the OKAPI_URL you must be connected to the VPN or the on-campus network.

6. Add a `.aws` directory with the config and credentials for the S3 bucket your want to use.
  Example credentials
  ```
  [default]
  aws_access_key_id = myaccess
  aws_secret_access_key = mysecret
  ```

7. Run `docker compose build` to build the customized Airflow image. (Note: the `usermod` command may take a while to complete when running the build.)
8. Run `docker compose up airflow-init` to initialize the Airflow database the first time you deploy Airflow.
9. Bring up Airflow, `docker compose up` to run the containers in the foreground. Use `docker compose up -d` to run as a daemon.
10. Access Airflow locally at http://localhost:8080. See [Authentication](#authentication) for how to sign in.
11. Log into the worker container using `docker exec -it libsys-airflow-airflow-worker-1 /bin/bash` to view the raw work files.

## Authentication

Which auth manager runs is set per environment by `AIRFLOW__CORE__AUTH_MANAGER`, not in
`airflow.cfg` — that file is baked into the Docker image, so anything set there would apply
everywhere. `compose.yaml` defaults local development to Keycloak; override it in your `.env`
to switch.

### Keycloak (the local default)

Points at the shared FOLIO Keycloak dev server (see `compose.yaml`) and its `sul` realm, so you
need VPN or the campus network. Set `AIRFLOW_KEYCLOAK_CLIENT_SECRET` in your `.env` from the
`airflow-sso` client's Credentials tab.

Airflow shares the `sul` realm with FOLIO rather than having its own. The `airflow-sso` client
carries the whole authorization model — roles, resources, scopes, policies, permissions — and is
exported from dev and imported into the other environments, so it is configured once rather than
rebuilt per environment. That export is the source of truth; the notes below describe its shape,
they are not a script to re-run.

#### Assigning roles

The five role names the auth manager recognizes are not configurable. They exist as **client roles
on `airflow-sso`**, not realm roles, so they cannot collide with FOLIO's own. As `create-all`
builds them, in non-team mode:

| Role | Covers |
|---|---|
| `Viewer` | GET, MENU, LIST on everything |
| `User` | Viewer, plus all methods on `Dag` and `Asset` |
| `Op` | Viewer, plus all methods on `Connection`, `Pool`, `Variable`, `Backfill` |
| `Admin` | Viewer, plus all extended methods on everything |
| `SuperAdmin` | identical to `Admin` unless multi-team mode is enabled |

`User` and `Op` are adjusted from that — see [Permission adjustments](#permission-adjustments).
People get `User` or `Admin`; `Op` is reserved for the service account, which is what keeps the
plugins' permissions independent of any human role.

Assign roles under Users → the user → **Role mapping**, filtered by clients. Nothing does this
automatically, in any environment. Permissions come from the token minted at login, so log out
and back in after a change; decisions are also cached briefly.

To debug a 403, use Authorization → **Evaluate** on the client with the user, resource, and scope
in question. It shows each permission's vote and which policy decided it.

#### The service account

The `airflow-sso` client needs **Service accounts roles** enabled, and its service account is a
separate user needing its own role assignment. The plugin apps call Airflow's public API as that
account through the `client_credentials` grant — see
`libsys_airflow/plugins/shared/airflow_api_client.py` — so without it every plugin that triggers
a DAG fails.

Assign it `Op` and nothing else: not `User`, and not `Admin`. The service account is an ordinary
Keycloak user governed by the same permissions a person is, so any role it shares with people
couples the two — tightening that role for people silently breaks plugin triggering. `Op` works
because nobody else is on it.

#### Permission adjustments

`create-all` produces four permissions — `ReadOnly`, `Admin`, `User` and `Op`. The exported client
carries these changes on top of them.

**`User`: read-only on DAGs.** Users reach DAGs through the plugin apps, which trigger runs as the
service account, so `User` needs no write scope. As created it is *resource*-based on `Dag` and
`Asset`, and a resource-based permission covers every scope of its resources, so replace it with
a scope-based one:

- Resources: `Dag`
- Authorization scopes: `GET`, `LIST` — omit `POST`, `PUT` and `DELETE`
- Policy: `Allow-User`

`LIST` is what makes the home page's Deadlines and History panels render instead of returning 403.

**`Op`: add the `Dag` resource.** This is where the service account gets the scopes the plugins
need to trigger, read and clear runs.

**`User-Custom` and `User-Views`:** let non-admins use the plugin apps and their nav entries. Both
are scope-based, and both need **Affirmative** — at Unanimous a permission with two policies
demands the user hold both roles.

- `User-Custom`: resource `Custom`, scopes `GET` and `POST`, policies `Allow-User` and `Allow-Op`
- `User-Views`: resource `View`, scopes `GET` and `LIST`, policy `Allow-User`

Finally, remove `Allow-User` from `ReadOnly`, so `User`s cannot click around the rest of Airflow
outside the plugins.

Re-running `create-all` reverts every change to the four permissions it owns, `User` and `Op`
included — which breaks plugin triggering until `Dag` is added back to `Op`. `User-Custom` and
`User-Views` survive, since `create-all` only manages permissions it creates by name.

#### Rebuilding the authorization model

Only needed when standing up a realm with no `airflow-sso` client to import. Create the five
client roles first — the CLI resolves them by name and fails if they are missing — then:

```
docker compose run --rm airflow-cli airflow keycloak-auth-manager create-all \
  --username <keycloak-admin> --password <keycloak-admin-password> --dry-run
```

Drop `--dry-run` once the output looks right. Then set two decision strategies to **Affirmative**
by hand, since `create-all` does not reliably apply them and either one left at Unanimous produces
a 403 that looks like a missing role: the resource server, under Authorization → Settings, and the
`Admin` permission, which has both `Allow-Admin` and `Allow-SuperAdmin` attached. Finally, check
each `Allow-<role>` policy is bound to the `airflow-sso` client role rather than a same-named
realm role.

### Simple auth (no identity provider)

To develop without VPN or a Keycloak client secret, add to your `.env`:

```
AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
```

`compose.yaml` already declares an `airflow` admin user via
`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS`. Its password is generated on first startup and
printed in the apiserver logs; it is stored in
`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`, which you can edit directly if you
want a fixed one. `AIRFLOW_VAR_API_USER` / `AIRFLOW_VAR_API_PASSWORD` (default `airflow` /
`airflow`) are how the plugin apps authenticate to the API under simple auth, so they need to
match that file.

## Deploying

### Prerequisites

1. Install `pip3` with `apt install python3-pip`
1. Install python virtual enviroments: `apt install python3.8-venv`
1. Install dependencies per `Dependency Management and Packaging` above
1. Install docker-compose in the poetry virtual environment: `poetry shell && pip3 install docker-compose`

### Tasks

List all the airflow tasks using `cap -AT airflow`

```
cap airflow:build          # run docker compose build for airflow
cap airflow:init           # run docker compose init for airflow
cap airflow:ps             # show running docker processes
cap airflow:restart        # restart airflow
cap airflow:start          # start airflow
cap airflow:stop           # stop and remove all running docker containers
cap airflow:stop_release   # stop old release and remove all old running docker containers
cap airflow:webserver      # restart webserver
```

#### Setup the list of databases for the database setup and migration tasks run via a capistrano deploy
List all the Alembic database migration tasks (see `Database migrations` below for more) using `cap -AT alembic`:

```
cap alembic:current  # Show current Alembic database migration
cap alembic:history  # Show Alembic database migration history
cap alembic:migrate  # Run Alembic database migrations
```

In `config/deploy.rb` at the top, add new default database set to the list of alembic_dbs:
```
set :alembic_dbs, ['vendor_loads', 'digital_bookplates', 'new_database', '...']
```

Or alternately, run any alembic task with the ALEMBIC_DBS environment variable set, e.g.
```
ALEMBIC_DBS='new_database, new_database2' cap dev alembic:migrate
```


### Do the first time you bring up Libsys-Airflow:

1. Log into the server, and run:
   1. `ksu`
   1. `apt install python3.11-venv libpq-dev`
   1. `cd /home/libsys`
   1. `python3.11 -m venv virtual-env`
   1. `touch /home/libsys/libsys-airflow/shared/config/.env`
   1. `vim /home/libsys/libsys-airflow/shared/config/.env` and add the following content:
      ```
      AIRFLOW_UID=214
      AIRFLOW_GROUP=0
      ```
1. In your local environment do `cap ${env} deploy deploy:install`
1. Visit https://sul-libsys-airflow-{env}.stanford.edu and complete the remaining steps. See shared_configs for instructions on getting the airflow admin user's password from vault.

## For subsequent deploys

`cap ${env} deploy`

This will stop and remove the docker images for the previous release and start up a new one.

## FOLIO Plugin

All FOLIO related code should be in the `folio` plugin. When developing
code in the plugin, you'll need to restart the `airflow-webserver` container
by running `cap {env} airflow:webserver` or ssh into the server and run `docker compose restart airflow-webserver`
to see changes in the running Airflow environment.

## Development

### Authorization for plugin apps

Airflow mounts plugin apps with no access control of its own, and hiding an app from the nav is
not a substitute: `GET /api/v2/plugins` gates the whole plugins menu on a single check and never
consults the individual apps. Each app guards itself with one app-level dependency, so a route
added later cannot forget it:

```python
from libsys_airflow.plugins.shared.auth import require_view_access

app = FastAPI(
    openapi_url=None,
    dependencies=[Depends(require_view_access("Boundwith CSV Upload"))],
)
```

`openapi_url=None` belongs with it. App-level dependencies reach only the routes the app
declares, so leaving the OpenAPI schema enabled leaves `/openapi.json`, `/docs` and `/redoc`
readable by anyone; these apps are browser views, not public APIs, so the endpoints are removed
rather than authenticated.

The view name is only a label, matched to the plugin's `external_views` entry by convention — no
auth manager can act on it. So this establishes that the caller is a signed-in user holding an
Airflow role, not which plugins they may use. Under Keycloak, non-admins reaching these apps is
what `User-Custom` and `User-Views` above provide.

### CSRF protection for plugin apps

Airflow 3 mounts each plugin app as its own FastAPI sub-application and provides no CSRF
protection, so a new app has to opt in. `libsys_airflow/plugins/shared/csrf.py` wraps
[fastapi-csrf-protect](https://github.com/aekasitt/fastapi-csrf-protect) (its `flexible` variant,
which accepts the token from either the form body or a header) so that opting in stays a one-line
change per app. In the app module:

```python
from libsys_airflow.plugins.shared.csrf import CSRFCookieMiddleware, csrf_protect

app.add_middleware(CSRFCookieMiddleware)

@app.post("/create", dependencies=[Depends(csrf_protect)])
def create(...):
    ...
```

and inside every `<form method="post">` in its templates:

```html
{{ csrf_field(request) }}
```

`csrf_field` and `csrf_token` are registered as Jinja globals by
`libsys_airflow.plugins.shared.utils.plugin_templates`, so apps that build their
`Jinja2Templates` with that helper get them for free. JavaScript that POSTs on its own must send
the token too, either as a `csrf_token` form field (`{{ csrf_token(request) }}`) or in an
`X-CSRFToken` header. In tests, `tests/csrf_helpers.csrf_test_client` presents a valid token on
every request; use a plain `TestClient` to assert the rejection path.

Two cookies are issued: `csrf_signed_token`, which is httponly and the one actually validated,
and `csrf_token`, which holds the matching unsigned value so a page render can reproduce the
token the form has to submit. Both are signed with `[api] secret_key` (set as
`AIRFLOW__API__SECRET_KEY` in `compose.prod.yaml`), which must be identical across API server
instances, are valid for eight hours, and are marked `Secure` wherever the deployment serves
HTTPS.

Tokens are bound to the authenticated user, so a pair minted for one user is rejected for
another and the middleware rolls the pair when the identity changes. Without that, an attacker
could mint a legitimate pair for themselves and overwrite the victim's cookies from any other
`stanford.edu` host, which is same-site as far as cookies are concerned.

### Support for Multiple Databases with Alembic
We are supporting multiple databases, Vendor Management App and Digital Bookplates, using alembic following
these [directions](https://alembic.sqlalchemy.org/en/latest/cookbook.html#run-multiple-alembic-environments-from-one-ini-file).

To run any `alembic` commands you need add the `--name` parameter followed by `vendor_loads` for the Vendor Management database
and `digital_bookplates` for the Digital Bookplates database.

### Local Database Creation and Migrations
How to get the digital_bookplates database running locally starting with fresh postgres volume:

1. Bring up airflow: `docker compose up`
1. In a different terminal window, set these variables:
```
export DATABASE_USERNAME="airflow"
export DATABASE_PASSWORD="airflow"
export DATABASE_HOSTNAME="localhost"
```
1.  Run (e.g. using the database name you want to migrate) `poetry run alembic --name digital_bookplates upgrade head`

#### Clearing Digital Bookplates Data

Assuming that you have populated the digital_bookplates database's digital_bookplates table with data, you can clear it out with:
```
poetry run bin/truncate_digital_bookplates
```

In a deployed environment, this can be run as:
```
docker exec -it 20230516183735-airflow-webserver-1 bin/truncate_digital_bookplates
```
where `20230516183735-airflow-webserver-1` is the container id.

### Vendor load plugin

Using and developing the vendor load plug in requires its own database. Ensure that the `vendor_loads` database exists in your local postgres and is owned by the airflow user.

To access the database in development, install
(e.g. `brew install postgresql`) and from your local terminal, run:

```
psql -h localhost -U airflow
```

To use psql in the docker container:

```
docker exec -it libsys-airflow-postgres-1 psql -U airflow
```

#### Database migrations

Using [Alembic](https://alembic.sqlalchemy.org/en/latest/) to manage database migrations for the `vendor_loads` database, changes to the
Models are autogenerated in a migration file in the `vendor_loads_migration/versions` directory. The Alembic migration requires the following environmental variables to be set either through a local `.env` file or
injected when using docker-compose:

- **DATABASE_USERNAME**
- **DATABASE_PASSWORD**
- **DATABASE_HOSTNAME**

When running locally you can use the `dotenv` command installed with the `python-dotenv` requirement to automatically put your `.env` variables into the environment.

To generate a migration script, first make the changes in the `models.py`
module and then run the following steps:

1. Set your shell to use the local poetry virtual environment: `poetry shell`
2. Run `dotenv run alembic --name vendor_loads revision --autogenerate -m "{short message describing change}"` (**NOTE**: not all changes to the model are detected, see this [note](https://alembic.sqlalchemy.org/en/latest/autogenerate.html#what-does-autogenerate-detect-and-what-does-it-not-detect) in the documentation)
3. After the migration script is created, run `dotenv run alembic upgrade head` to apply your latest changes to the database.

If you prefer not to use `poetry shell` you can use `poetry run` along with `dotenv` instead: e.g. `poetry run dotenv run alembic --name vendor_loads upgrade head`. Or you can simply put the `DATABASE_*` environment variables into your shell via another means.

To fix multiple heads: `poetry run alembic --name vendor_loads merge heads -m "merge <revision 1> and <revision 2>"`

#### Clearing out VendorFiles for Development
Assuming you have populated VendorFiles table and want to re-fetch vendor files, clear out the database table with:
```
poetry run bin/truncate_vendor_files
```

#### Seeding Vendors

Assuming that you have put Folio related environment variables in your `.env` file you can load all organizations (over 3,000) with:

```
dotenv run bin/seed_vendors
```

To load a limited set of only six vendors, use `--limited`:

```
dotenv run bin/seed_vendors --limited
```

In a deployed environment, the full load can be run as:
```
docker exec -it libsys_airflow-airflow-webserver-1 bin/seed_vendors
```

#### Refreshing Vendor Data

Assuming that you have put Folio related environment variables in your `.env` file you can look up acquisitions unit names in Folio and populate the database:
```
dotenv run bin/refresh_vendors
```

This can be run as a dry run with `-d` or `--dry`.

In a deployed environment, this can be run as:
```
docker exec -it 20230516183735-airflow-webserver-1 bin/refresh_vendors
```
where `20230516183735-airflow-webserver-1` is the container id.

#### Poetry Lock Merge Conflicts
If when doing a rebase you encounter a merge conflict with the `poetry.lock` file and assuming you have added/updated
dependencies to the `pyproject.toml`, run the following commands:

- `git checkout --theirs poetry.lock`
- `poetry lock --no-update`

## Testing

1. Install dependencies per `Dependency Management and Packaging` above
1. Drop into the poetry virtual environment: `poetry shell` (alternatively, if you don't want to drop into `poetry shell`, you can run commands using `poetry run my_cmd`, akin to `bundle exec my_cmd`)

Install the test database (sqlite):
`airflow db init`

Then, to run the test suite, use [pytest](https://docs.pytest.org/).
`poetry run pytest`

To see stdout or stderr add the `-rP` flag:
`poetry run pytest -rP`

### Flake8 (Python linter)

Flake8 configuration is in `setup.cfg` (flake8 cannot be configured with `pyproject.toml`).

For CI, see the `Lint with flake8` step in `.github/workflows/lint.yml`.

To run the flake8 linter: `poetry run flake8`

### Black (Python formatter)

Black configuration is in `pyproject.toml`.

For CI, see the `Format with black` step in `.github/workflows/lint.yml` github action. It has been configured to fail if there are any violations.

To run the black formatter to fix any violations:

`poetry run black .`

### Mypy (Python type checker)

[Mypy](https://github.com/python/mypy) is a static type checker.  Configuration is in `pyproject.toml`.

To run the mypy type checker on the codebase: `poetry run mypy libsys_airflow tests`.

The type checker will complain if assignments, return types, parameter types, etc are inconsistently used, e.g. if an int is provided for a string param, a function doesn't return the type it claims to, etc (helpful for documenting function signatures correctly, and for sussing out issues like inconsistent types for variable assignment or function return, which can be confusing at best, and bug prone at worst).

Type declarations are only used in testing and type checking, and do not affect runtime behavior.

If you run into something where the type checker complains and you're sure that the usage is ok, you can comment the line with `# type: ignore` to quiet the type checker for that line.  As with linting, use your best judgement as to whether an exception is preferable to mollifying the checker.

The type checker is not currently wired up in CI as a required check, but you may still find it useful for catching lurking consistency issues.

## Connections

### SFTP

SFTP connections may have a password or a key file. The key files should be named with the hostname (e.g., `sftp.amalivre.fr`) and placed in `vendor-keys/` directory.

## DAGs

### data_fetcher

* Use the `filename_regex` value `CNT-ORD` for special Gobi file filtering (filter .ord files that don't have a corresponding .cnt file).
* Set the `download_days_ago` airflow variable to limit downloads to a specific time period (default is 10 days).

## Cleanup of Logs and Workfiles

Edit `plugins/remove_old_files_cron.py` to set the days and times to execute the removal of old files. Set the `mtime` flag to the maximum number of days for file retention.

On the deployment server run `cap {stage} deploy write_crontab` or on the airflow server, `source virtual-env/bin/activate`, then from within the current project directory run `crontab -r` and then `poetry run python plugins/remove_old_files_cron.py`. Do `crontab -l` to check the crontab.
