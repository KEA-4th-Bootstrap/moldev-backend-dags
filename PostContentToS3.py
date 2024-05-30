import sys
import subprocess
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pymongo'])
import pymongo
from pymongo import MongoClient
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'boto3'])
import boto3
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
import csv


def read_moldev_ids_from_mysql():
    mongo_url = Variable.get("POST_MONGO_URL")
    client = MongoClient(mongo_url)
    db = client['moldb']
    collection = db['post']

    # 현재 시간과 24시간 이전 시간 계산
    current_time = datetime.utcnow()
    time_threshold = current_time - timedelta(hours=3)

    recent_posts = collection.find({'last_modified_date': {'$gte': time_threshold}})
    
    return [recent_post['moldev_id'] for recent_post in recent_posts]


def save_moldev_ids_to_s3(moldev_ids):
    moldev_csv = ', '.join(moldev_ids)
    # CSV 파일로 저장
    file_name = 'member/moldev_ids.csv'

    # S3에 업로드
    s3 = boto3.client('s3',
                      aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
                      aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
                      region_name=Variable.get("AWS_DEFAULT_REGION")
                      )
    bucket_name = 'moldev-s3-bucket'
    
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=moldev_csv)

def save_moldev_ids():
    user_ids = read_moldev_ids_from_mysql()
    save_moldev_ids_to_s3(user_ids)

def read_user_ids_from_s3():    
    s3 = boto3.client('s3',
                      aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
                      aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
                      region_name=Variable.get("AWS_DEFAULT_REGION")
                      )
    bucket_name = 'moldev-s3-bucket'
    file_name = 'member/moldev_ids.csv'
    local_file_name = '/tmp/moldev_ids.csv'

    # /tmp 디렉토리가 없는 경우 생성
    os.makedirs('/tmp', exist_ok=True)

    # os.chmod(local_file_name, 777)
    # S3에서 파일 다운로드
    s3.download_file(bucket_name, file_name, local_file_name)
    moldev_ids = []
    with open(local_file_name, mode='r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            moldev_ids = row
            
    return moldev_ids


def read_posts_from_mongo_and_save_to_s3(moldev_id):
    category_map = {'1': '대외활동',
                    '2': '프로젝트',
                    '3': '수상이력',
                    '4': '트러블슈팅',
                    '5': '자기소개'}

    moldev_id = moldev_id.lstrip()
    mongo_url = Variable.get("POST_MONGO_URL")
    client = MongoClient(mongo_url)
    db = client['moldb']
    collection = db['post']

    posts = collection.find({'moldev_id': moldev_id})
    posts_str = ""
    for i in posts:
        posts_str += '\n# 게시글 제목 : '+str(i['title'])
        posts_str += '\n### 게시글 카테고리 : '+ category_map.get(str(i['category']))
        posts_str += '\n게시글 작성일 : '+str(i['create_date'])
        posts_str += '\n\n게시글 주소 : '+str(i['front_url'])
        posts_str += '\n\n게시글 내용 : '+str(i['profile_content'])
        posts_str += '\n\n---\n'

    with open('new.md', 'w', encoding='utf-8') as file:
        file.write(posts_str)

    # S3에 업로드
    s3 = boto3.client('s3',
                      aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
                      aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
                      region_name=Variable.get("AWS_DEFAULT_REGION")
                      )

    bucket_name = 'moldev-s3-bucket'
    file_name = f'post/{moldev_id}_posts.md'
    s3.upload_file('new.md', bucket_name, file_name)


def process_posts_from_s3():
    user_ids = read_user_ids_from_s3()
    for user_id in user_ids:
        read_posts_from_mongo_and_save_to_s3(user_id)


# Airflow DAG 정의
default_args = {
    'depends_on_past': False,
    'email': ['ojy09291@naver.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'change_post_to_s3',
    default_args=default_args,
    description='사용자의 게시글 정보들을 s3로 옮깁니다.',
    schedule_interval=timedelta(hours=1.5),
    start_date=datetime(2024, 5, 26),
    catchup=False,
    tags=['post'],
)

with dag:
    fetch_and_save_moldev_ids_task = PythonOperator(
        task_id='save_moldev_ids',
        python_callable=save_moldev_ids,
    )

    process_posts_from_s3_task = PythonOperator(
        task_id='process_posts_from_s3',
        python_callable=process_posts_from_s3,
    )

    post_to_s3_slack = SlackWebhookOperator(
        task_id='post_to_s3_slack',
        slack_webhook_conn_id='slack-webhook',  # Airflow connection id for Slack web    hook
        message="Moldev posts have been processed and saved to S3.",
    )

    fetch_and_save_moldev_ids_task >> process_posts_from_s3_task >> post_to_s3_slack
