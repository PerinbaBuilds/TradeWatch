"""Apache Airflow DAG — TradeWatch daily surveillance ETL.

Orchestrates the batch/scale layer end-to-end, once per day:

    land raw trades (HDFS)
        → Spark batch backtest (curated anomalies, Parquet on HDFS)
        → Hadoop MapReduce cross-check (independent batch scan)
        → register Hive partitions
        → load anomalies into Snowflake (gold layer)
        → data-quality check

Drop this file in your Airflow ``dags/`` folder. Connections/paths are
parameterised via Airflow Variables so nothing is hard-coded.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "tradewatch",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

REPO = "{{ var.value.get('tradewatch_repo', '/opt/tradewatch') }}"
HDFS_TRADES = "/tradewatch/trades/dt={{ ds }}"
HDFS_ANOMALIES = "/tradewatch/anomalies/dt={{ ds }}"


def _data_quality_check(**context) -> None:
    """Fail the run if the day produced zero curated anomalies (pipeline broke)."""
    import subprocess

    ds = context["ds"]
    out = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", f"/tradewatch/anomalies/dt={ds}/_SUCCESS"],
        capture_output=True,
    )
    if out.returncode != 0:
        raise ValueError(f"no _SUCCESS marker for anomalies partition dt={ds}")


with DAG(
    dag_id="tradewatch_daily_etl",
    description="Daily trade-surveillance batch ETL (Spark + Hadoop + Hive + Snowflake)",
    default_args=DEFAULT_ARGS,
    schedule="0 2 * * *",           # 02:00 UTC daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tradewatch", "surveillance", "big-data"],
) as dag:

    spark_backtest = BashOperator(
        task_id="spark_batch_backtest",
        bash_command=(
            f"cd {REPO} && spark-submit spark/batch_backtest.py "
            f"--input hdfs://{HDFS_TRADES} --output hdfs://{HDFS_ANOMALIES} --format parquet"
        ),
    )

    mapreduce_crosscheck = BashOperator(
        task_id="hadoop_mapreduce_crosscheck",
        bash_command=(
            f"cd {REPO} && hadoop/run_streaming.sh {HDFS_TRADES} /tradewatch/mr_anomalies/dt={{{{ ds }}}}"
        ),
    )

    register_hive = BashOperator(
        task_id="register_hive_partitions",
        bash_command=f"cd {REPO} && hive -e 'MSCK REPAIR TABLE tradewatch.anomalies;'",
    )

    load_snowflake = BashOperator(
        task_id="load_snowflake_gold",
        bash_command=(
            f"cd {REPO} && python warehouse/snowflake/load_snowflake.py "
            f"--input hdfs://{HDFS_ANOMALIES} --dt {{{{ ds }}}}"
        ),
    )

    quality_gate = PythonOperator(
        task_id="data_quality_check",
        python_callable=_data_quality_check,
    )

    spark_backtest >> [mapreduce_crosscheck, register_hive] >> load_snowflake >> quality_gate
