```md
# Airflow ETL: SQL Server → PostgreSQL

This repository contains an **Apache Airflow DAG** that performs an ETL pipeline from **SQL Server** to **PostgreSQL**, then sends an email notification on success (and on failure via Airflow defaults).

---

## DAG Details
- **DAG ID:** `etl_sqlserver_to_postgres`
- **Schedule:** Manual trigger (`schedule_interval=None`)
- **Start Date:** `2025-11-14`
- **Catchup:** `False`
- **Tags:** `etl`, `sqlserver`, `postgres`

---

## Pipeline Flow
1. **Extract** data from SQL Server (`dbo.SourceTable`)
2. **Transform** the data (clean name, convert amount)
3. **Load** into PostgreSQL (`target_table`)
4. **Notify** success via email

Task dependency:
```

extract → transform → load → notify_success

````

---

## Configuration (default_args)
- Retries once on failure (`retries=1`)
- 5 minutes delay between retries
- Sends email on failure (`email_on_failure=True`)
- No email on retry (`email_on_retry=False`)

---

## Extract (SQL Server)
- Uses `pytds` to connect to SQL Server:
  - `server="host.docker.internal"`
  - `database="SourceDB"`
  - `port=1433`
- Reads:
```sql
SELECT Id, Name, CreatedAt, Amount
FROM dbo.SourceTable;
````

* Returns rows as `list[dict]` using `as_dict=True`.

---

## Transform

Applied transformations:

* `Name`: `strip()` + `title()` (if not null)
* `Amount`: cast to `float`, default `0.0` if null
* Renames columns to match target schema:

  * `Id → id`, `Name → name`, `CreatedAt → created_at`, `Amount → amount`

---

## Load (PostgreSQL)

* Uses `psycopg2` to connect:

  * `host="postgres"`, `port=5432`
  * `dbname="airflow"`, `user="airflow"`, `password="airflow"`
* Creates table if not exists:

```sql
CREATE TABLE IF NOT EXISTS target_table (
  id         INTEGER PRIMARY KEY,
  name       TEXT,
  created_at TIMESTAMP,
  amount     DOUBLE PRECISION
);
```

* Truncates the table each run:

```sql
TRUNCATE TABLE target_table;
```

* Inserts all rows using `executemany`.

---

## Email Notification

### Success

After loading completes, `EmailOperator` sends a success email:

* Subject: `ETL etl_sqlserver_to_postgres succeeded`
* Includes:

  * DAG id
  * execution date
  * load result from XCom: `{{ ti.xcom_pull(task_ids='load') }}`

### Failure

Airflow automatically emails the addresses in `default_args["email"]` when any task fails.

---

## Requirements

* Apache Airflow
* `pytds`
* `psycopg2` / `psycopg2-binary`

Example install:

```bash
pip install pytds psycopg2-binary
```

---

## Notes

* For security, move credentials to **Airflow Connections** or environment variables instead of hard-coding.
* If you want incremental loads, remove `TRUNCATE` and implement upsert/merge logic.

```
```
