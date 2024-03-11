# Airflow

![스크린샷 2024-03-11 103432](https://github.com/devcourse-final-prj/airflow/assets/75061809/8fc23f50-d992-49bf-a39f-25308aaabb1d)

- EC2 (webserve/ scheduler , Celery Worker), RDS, Elasticache, S3로 구성
    - ALB, NGW, Bastion host 는 public, 나머지 airflow 관련 인스턴스는 private으로 설정
- 작업에 대한 접근은 Bation host (public) 을 통해 접근, 웹페이지에 대한 접근은 ALB를 통해서 8080 port만 접속 가능하도록 설정
- Celery Executor(Redis)로 작업 할당 및 진행
- 작업 후 저장 되는 곳은 DAG 마다 다르게 설정 , 저장된 데이터는 Superset 혹은 Django 웹페이지로 구현
- 작업목록
  - daily_financial_data_to_rds_dag.py
    - 머신러닝데이터 수집
  - stock_daily_closing_price.py
    - 이동평균선 데이터 수집
  - calc_profit_loss.py
    - 가격 비교 데이터 수집 
