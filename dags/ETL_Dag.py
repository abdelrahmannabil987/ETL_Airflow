from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.email import EmailOperator

import pytds
import psycopg2


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),

    "email": ["abdelrahmannabil987@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="etl_sqlserver_to_postgres",
    default_args=default_args,
    start_date=datetime(2025, 11, 14),
    schedule_interval=None,  
    catchup=False,
    tags=["etl", "sqlserver", "postgres"],
):

    @task
    def extract():
        conn = pytds.connect(
            server="host.docker.internal",   
            database="SourceDB",
            user="airflow_user",
            password="Strong_Passw0rd!",
            port=1433,
            as_dict=True,
        )
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT Id,
                           Name,
                           CreatedAt,
                           Amount
                    FROM dbo.SourceTable;
                    """
                )
                rows = cur.fetchall()   # list[dict]
        return rows

    @task
    def transform(rows):
        transformed = []
        for row in rows:
            name = row["Name"]
            if name:
                name = name.strip().title()

            amount = row["Amount"]
            amount = float(amount) if amount is not None else 0.0

            transformed.append(
                {
                    "id": row["Id"],
                    "name": name,
                    "created_at": row["CreatedAt"],
                    "amount": amount,
                }
            )
        return transformed

    @task
    def load(rows):
        if not rows:
            return "No rows to load"

        conn = psycopg2.connect(
            host="postgres",   
            port=5432,
            dbname="airflow",
            user="airflow",
            password="airflow",
        )

        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS target_table (
                        id         INTEGER PRIMARY KEY,
                        name       TEXT,
                        created_at TIMESTAMP,
                        amount     DOUBLE PRECISION
                    );
                    """
                )
                cur.execute("TRUNCATE TABLE target_table;")

                insert_sql = """
                    INSERT INTO target_table (id, name, created_at, amount)
                    VALUES (%s, %s, %s, %s);
                """

                data = [
                    (r["id"], r["name"], r["created_at"], r["amount"])
                    for r in rows
                ]
                cur.executemany(insert_sql, data)

        return f"Loaded {len(rows)} rows into target_table"

    extracted = extract()
    transformed = transform(extracted)
    load_task = load(transformed)

    notify_success = EmailOperator(
        task_id="notify_success",
        to=["abdelrahmannabil987@gmail.com"],   
        subject="✅ ETL etl_sqlserver_to_postgres succeeded",
        html_content="""
        DAG {{ dag.dag_id }} finished successfully.<br>
        Execution date: {{ ds }}<br>
        Result from load task: {{ ti.xcom_pull(task_ids='load') }}
        """,
    )

    load_task >> notify_success
