from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    'depends_on_past': False,
    'email': ['ojy09291@naver.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

with DAG(
    'moldb_dump_dag',
    default_args=default_args,
    description='게시글, 멤버, 신고 db의 database의 dump 파일을 만듭니다.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 6, 10),
    tags=['backup'],
    catchup=False,
) as dag:

    create_member_dump = SSHOperator(
        task_id="create_member_dump",
        ssh_conn_id='my_memberdb_connection',
        command=(
            f'docker exec db001 /usr/bin/mysqldump -u {Variable.get("MYSQL_USER")} -p{Variable.get("MYSQL_PASSWORD")} {Variable.get("MYSQL_DATABASE")} > /home/ubuntu/db_backup/mysqldump.sql'
        ),
    )
    
    create_post_dump = SSHOperator(
        task_id="create_post_dump",
        ssh_conn_id='my_postdb_connection',
        command=(
            f'docker exec db001 /usr/bin/mysqldump -u {Variable.get("MYSQL_USER")} -p{Variable.get("MYSQL_PASSWORD")} {Variable.get("MYSQL_DATABASE")} > /home/ubuntu/db_backup/mysqldump.sql'
        ),
    )
    
    create_report_dump = SSHOperator(
        task_id="create_report_dump",
        ssh_conn_id='my_reportdb_connection',
        command=(
            f'docker exec db001 /usr/bin/mysqldump -u {Variable.get("MYSQL_USER")} -p{Variable.get("MYSQL_PASSWORD")} {Variable.get("MYSQL_DATABASE")} > /home/ubuntu/db_backup/mysqldump.sql'
        ),
    )
    

    [create_member_dump, create_post_dump, create_report_dump]
