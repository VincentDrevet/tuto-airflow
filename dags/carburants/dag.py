from airflow.decorators import task, dag
import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models.param import Param 


@dag(schedule=None, start_date=pendulum.datetime(2023,12,16, tz="Europe/Paris"), catchup=False, tags=["tutorial"], 
     params={"database_db": Param("db", type="string"), "database_user": Param("postgres", type="string"),  "database_password": Param("secret", type="string")})
def carburants_dag():

    # 1) Load dim_region and dim_departement table in the datawarehouse
    
    load_dim = DockerOperator(
        task_id="seeding_dim",
        image="mydbt",
        docker_url="tcp://docker-socket-proxy:2375",
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=[Mount(type="bind", read_only=False, source="/Users/shiki/Documents/airflow/dags/carburants/carburants_analytics", target="/work")],
        command="dbt seed",
        network_mode="airflow_default",
        environment={"DB_USER": "{{ params.database_user }}", "DB_PASSWORD": "{{ params.database_password }}", "DB_DATABASE": "{{ params.database_db }}"}
    )

    # 2) Data quality step

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

    load_dim >> data_quality

carburants_dag()

    
    
        