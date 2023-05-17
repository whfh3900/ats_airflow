# RealATS? Airflow 스케쥴링

## 프로젝트 개요
이 프로젝트는 RealATS? 기능을 Airflow 스케쥴링을 통해 구현한 것입니다.
<br><br><br>

## 요구사항
- Python : 3.8
- Apache Airflow : 2.5.3
- ats-ckonlpy : 1.0.0
- keras : 2.10.0
- pandas : 1.5.0
- PyMySQL : 1.0.3
- tensorflow : 2.10.0
<br><br><br>

## 설정
- Airflow 설정 파일의 경로: airflow.cfg
- 스케줄러 타입: Sequential
- 실행 주기: */10 * * * * (매일 10초에 한 번씩)(미정)
<br><br><br>

## DAG 파일 구조
- dags/ 디렉토리에 각각의 DAG 파일이 저장됩니다.
- dags/ats_schedule_dag.py: 예제 DAG 파일입니다.
- dags/ats_scheduler_tasks.py: 태스크 정의 파일입니다.
- dags/ats_module: RealATS? 모듈 폴더입니다.
<br><br><br>

## 사용법
```
$ airflow scheduler -D
$ airflow webserver -D -p <port>
```
스케쥴러와 웹서비스를 데모버젼으로 띄우고 해당 포트로 접속하시면 됩니다.
