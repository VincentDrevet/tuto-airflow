from airflow.decorators import task, dag
import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models.param import Param 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator


@dag(schedule=None, start_date=pendulum.datetime(2023,12,16, tz="Europe/Paris"), catchup=False, tags=["tutorial"], 
     params={"database_db": Param("db", type="string"), "database_user": Param("postgres", type="string"),  "database_password": Param("secret", type="string"), "dbt_target": Param("dev", type="string")})
def carburants_dag():

    # 1) Load dim_region and dim_departement table in the datawarehouse
    
    load_dim = DockerOperator(
        task_id="seeding_dim",
        image="mydbt",
        docker_url="tcp://docker-socket-proxy:2375",
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=[Mount(type="bind", read_only=False, source="/Users/shiki/Documents/airflow/dags/carburants/carburants_analytics", target="/work")],
        command="dbt seed -t {{ params.dbt_target }}",
        network_mode="airflow_default",
        environment={"DB_USER": "{{ params.database_user }}", "DB_PASSWORD": "{{ params.database_password }}", "DB_DATABASE": "{{ params.database_db }}"}
    )

    # 2) Load raw_data

    @task.docker(docker_url="tcp://docker-socket-proxy:2375", image="mypython", mount_tmp_dir=False, auto_remove=True, network_mode="airflow_default",
                 environment={"DB_USER": "{{ params.database_user }}", "DB_PASSWORD": "{{ params.database_password }}", "DB_DATABASE": "{{ params.database_db }}"})
    def load_raw_data():
        # Step1 download csv file from opendata.gouv.fr
        # Step2 parse csv file and load data in postgres datawarehouse

        import requests
        import os
        import pandas
        from sqlalchemy import create_engine
        from datetime import datetime
        from zoneinfo import ZoneInfo

        engine = create_engine(f'postgresql+psycopg2://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@datawarehouse/{os.environ.get('DB_DATABASE')}')

        url = "https://www.data.gouv.fr/fr/datasets/r/edd67f5b-46d0-4663-9de9-e5db1c880160"
        r = requests.get(url, allow_redirects=True)
        open('raw_data.csv', 'wb').write(r.content)

        df = pandas.read_csv('raw_data.csv', delimiter=';')
        df["DT_TRANSACTION"] = datetime.now(ZoneInfo("Europe/Paris"))

        with engine.begin() as connection:
            df.to_sql("raw_data", con=connection, if_exists="append")

    # 3) DBT staging
    dbt_staging = DockerOperator(
        task_id="dbt_staging",
        image="mydbt",
        docker_url="tcp://docker-socket-proxy:2375",
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=[Mount(type="bind", read_only=False, source="/Users/shiki/Documents/airflow/dags/carburants/carburants_analytics", target="/work")],
        command="dbt run -t {{ params.dbt_target }} --select staging.carburants",
        network_mode="airflow_default",
        environment={"DB_USER": "{{ params.database_user }}", "DB_PASSWORD": "{{ params.database_password }}", "DB_DATABASE": "{{ params.database_db }}"}
    )

    # 4) DBT intermediate
    dbt_intermediate = DockerOperator(
        task_id="dbt_intermediate",
        image="mydbt",
        docker_url="tcp://docker-socket-proxy:2375",
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=[Mount(type="bind", read_only=False, source="/Users/shiki/Documents/airflow/dags/carburants/carburants_analytics", target="/work")],
        command="dbt run -t {{ params.dbt_target }} --select intermediate.carburants",
        network_mode="airflow_default",
        environment={"DB_USER": "{{ params.database_user }}", "DB_PASSWORD": "{{ params.database_password }}", "DB_DATABASE": "{{ params.database_db }}"}
    )

    # 5) get distinct DT_TRANSACTION
    @task.docker(docker_url="tcp://docker-socket-proxy:2375", image="mypython", mount_tmp_dir=False, auto_remove=True, network_mode="airflow_default",
                environment={"DB_USER": "{{ params.database_user }}", "DB_PASSWORD": "{{ params.database_password }}", "DB_DATABASE": "{{ params.database_db }}"})
    def get_dt_transaction_distinct() -> int:

        from sqlalchemy import create_engine, text
        import os

        engine = create_engine(f'postgresql+psycopg2://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@datawarehouse/{os.environ.get('DB_DATABASE')}')

        with engine.connect() as connection:
            
            result = connection.execute(text('select count(*) from (select distinct("DT_TRANSACTION") from stg_fuel_price)'))
            count = result.first()

            return count[0]

    # 6) DBT fact
    dbt_marts = DockerOperator(
        task_id="dbt_marts",
        image="mydbt",
        docker_url="tcp://docker-socket-proxy:2375",
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=[Mount(type="bind", read_only=False, source="/Users/shiki/Documents/airflow/dags/carburants/carburants_analytics", target="/work")],
        command="dbt run -t {{ params.dbt_target }} --select marts.carburants",
        network_mode="airflow_default",
        environment={"DB_USER": "{{ params.database_user }}", "DB_PASSWORD": "{{ params.database_password }}", "DB_DATABASE": "{{ params.database_db }}"}
    )

    # 7) Data quality step

    data_quality = DockerOperator(
        task_id="data_quality",
        image="mysoda",
        docker_url="tcp://docker-socket-proxy:2375",
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=[Mount(type="bind", read_only=False, source="/Users/shiki/Documents/airflow/dags/carburants/soda", target="/work")],
        command="soda scan -d datawarehouse -c configuration.yml checks.yml",
        network_mode="airflow_default",
        environment={"DB_USER": "{{ params.database_user }}", "DB_PASSWORD": "{{ params.database_password }}", "DB_DATABASE": "{{ params.database_db }}"}
    )

    load_dim >> load_raw_data() >> dbt_staging >> dbt_intermediate >> dbt_marts >> data_quality

carburants_dag()

    
    
        