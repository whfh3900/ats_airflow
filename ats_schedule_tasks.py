import pymysql
import paramiko
import os
from dotenv import load_dotenv
load_dotenv()
from ast import literal_eval
from ats_module.text_preprocessing import Nickonlpy, read_csv_for_tagging
from ats_module.text_tagging import NicWordTagging
import pandas as pd

# mysql 접속 상태코드:0110 인 상단 row 반환
def connect_mysql_return_values(**kwargs):

    # mysql 접속
    connection = pymysql.connect(host=str(os.getenv("mysql_host")), 
                                user=str(os.getenv("mysql_user")), 
                                password=str(os.getenv("mysql_password")), 
                                db=str(os.getenv("mysql_db")), 
                                charset='utf8')
    cursor = connection.cursor()

    # select 조건문으로 데이터 찾기
    # PRCS_CD가 PRCS_CD가 0110인 row
    query = "select SEQ_NO, CORP_ID, MODEL_ID, UPLD_FILE_NM, REG_DT from TB_TAGG_HIST_copy where PRCS_CD = '0110' LIMIT 1;"

    cursor.execute(query)
    
    if len(cursor.fetchall()) == 1:
        # select 조건문으로 데이터 찾기
        # PRCS_CD가 PRCS_CD가 0110인 row
        query = "select SEQ_NO, CORP_ID, MODEL_ID, UPLD_FILE_NM, REG_DT from TB_TAGG_HIST_copy where PRCS_CD = '0110' LIMIT 1;"
        cursor.execute(query)
        data = [row for row in cursor.fetchall()][0]

        # 연결종료
        connection.commit()
        connection.close()

        # 데이터 정의
        seq_no = data[0]
        corp_id = data[1]
        model_id = data[2]
        upld_file_nm = data[3]
        reg_dt = data[4].strftime("%Y-%m-%dT%H:%M:%SZ")

        # local 적요분류 파일경로
        basename = os.path.basename(data[3])
        file_path = "/home/manager/django_api/media/%s/%s/%s"%(corp_id, reg_dt, basename)

        data = dict()
        data["SEQ_NO"] = seq_no
        data["CORP_ID"] = corp_id
        data["MODEL_ID"] = model_id
        data["UPLD_FILE_NM"] = upld_file_nm
        data["REG_DT"] = reg_dt
        data["FILE_PATH"] = file_path
        
        kwargs['ti'].xcom_push(key='data', value=data)
        
        return "text_preprocessing"
    
    elif len(cursor.fetchall()) == 0:
        return "task_pass"
    else:
        return "task_error"
    

# 실행할 task가 없을때
def task_pass():
    print("실행할 Task가 없습니다.")
    return False

def task_error():
    print("task에 에러가 있습니다.")
    return False

def complete(**kwargs):
    data = kwargs['ti'].xcom_pull(key="data")
    # 상태코드 변경(적요분류 시작)
    data["STATE"] = "0700"
    data["MESSAGE"] = "모든 프로세스가 종료되었습니다."
    change_state_code(data["SEQ_NO"], data["STATE"])
    print(data["MESSAGE"])

# 전처리
def preprocessing(**kwargs):
    data = kwargs['ti'].xcom_pull(key="data")
    print(data, type(data))

    # 상태코드 변경(전처리 시작)
    data["STATE"] = "0200"
    data["MESSAGE"] = "전처리를 시작합니다."
    change_state_code(data["SEQ_NO"], data["STATE"])
    
    # 파일 불러오기
    # df = read_csv_for_tagging(data["FILE_PATH"])
    df = pd.read_csv(data["FILE_PATH"], encoding="utf-8-sig")

    try:
        # 전처리 시작
        nk = Nickonlpy()
        df["적요"] = df["적요"].apply(lambda x: nk.lambda_preprocessing(x))
        # 전처리된 데이터 덮어쓰기
        df.to_csv(data["FILE_PATH"], encoding="utf-8-sig", index = False)
    except Exception as e:
        data["STATE"] = "0211"
        data["MESSAGE"] = "전처리중 알 수 없는 오류가 발생했습니다. %s"%e
        # 상태코드 변경(전처리 에러)
        change_state_code(data["SEQ_NO"], data["STATE"])
        return 'task_error'

    # 상태코드 변경
    data["STATE"] = "0210"
    data["MESSAGE"] = "전처리를 성공적으로 끝마쳤습니다."
    # 상태코드 변경(전처리 완료)
    change_state_code(data["SEQ_NO"], data["STATE"])
    
    kwargs['ti'].xcom_push(key='data', value=data)
    
    return 'text_classification'



# 적요분류 및 저장
def text_classification(**kwargs):
    data = kwargs['ti'].xcom_pull(key="data")
    
    # 상태코드 변경(적요분류 시작)
    data["STATE"] = "0300"
    data["MESSAGE"] = "적요분류를 시작합니다."
    change_state_code(data["SEQ_NO"], data["STATE"])

    # 파일 불러오기
    df = read_csv_for_tagging(data["FILE_PATH"])

    try:
        # 적요분류 시작
        nwt = NicWordTagging(data['MODEL_ID'])
        df = nwt.split_transaction_df(df)
        # 적요분류된 데이터 덮어쓰기
        df.to_csv(data["FILE_PATH"], encoding="utf-8-sig", index = False)
    except Exception as e:
        # 상태코드 변경(적요분류 에러)
        data["STATE"] = "0311"
        data["MESSAGE"] = "적요분류중 알 수 없는 오류가 발생했습니다. %s"%e
        change_state_code(data["SEQ_NO"], data["STATE"])
        return 'task_error'

    # 상태코드 변경(적요분류 완료)
    data["STATE"] = "0310"
    data["MESSAGE"] = "적요분류를 성공적으로 끝마쳤습니다."
    change_state_code(data["SEQ_NO"], data["STATE"])
    
    kwargs['ti'].xcom_push(key='data', value=data)

    return 'file_transfer'



# 웹서버에 파일 전송
def file_transfer(**kwargs):
    data = kwargs['ti'].xcom_pull(key="data")
    print(data, type(data))

    # 상태코드 변경(파일전송 시작)
    data["STATE"] = "0500"
    data["MESSAGE"] = "파일전송을 시작합니다."
    change_state_code(data["SEQ_NO"], data["STATE"])
    
    try:
        # ssh 접속 및 sftp 열기
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(str(os.getenv("ssh_host")), 
                        str(os.getenv("ssh_port")), 
                        str(os.getenv("ssh_username")), 
                        str(os.getenv("ssh_password")))
        sftp_client = ssh_client.open_sftp()
        
        basename = os.path.basename(data["UPLD_FILE_NM"])
        remote_file_path = "/home/manager/work/file/result/"+basename
        sftp_client.put(data["FILE_PATH"], remote_file_path)
        
        # sftp 및 ssh 닫기
        sftp_client.close()
        ssh_client.close()
        
        # 파일 삭제
        os.remove(data["FILE_PATH"])
        
    except Exception as e:
        # 상태코드 변경(파일전송 에러)
        data["STATE"] = "0511"
        data["MESSAGE"] = "파일전송중 알 수 없는 오류가 발생했습니다. %s"%e
        print(data["MESSAGE"])
        change_state_code(data["SEQ_NO"], data["STATE"])
        return 'task_error'

    # 상태코드 변경(파일전송 완료)
    data["STATE"] = "0510"
    data["MESSAGE"] = "파일전송 완료."
    change_state_code(data["SEQ_NO"], data["STATE"])
    kwargs['ti'].xcom_push(key='data', value=data)

    return "file_remove"


# 파일 삭제
def file_remove(**kwargs):
    data = kwargs['ti'].xcom_pull(key="data")
    print(data, type(data))
    
    # 상태코드 변경(파일전송 시작)
    data["STATE"] = "0600"
    data["MESSAGE"] = "파일삭제를 시작합니다."
    change_state_code(data["SEQ_NO"], data["STATE"])


    try:
        # ssh 접속 및 sftp 열기
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(str(os.getenv("ssh_host")), 
                        str(os.getenv("ssh_port")), 
                        str(os.getenv("ssh_username")), 
                        str(os.getenv("ssh_password")))
        sftp_client = ssh_client.open_sftp()
        
        sftp_client.remove(data["UPLD_FILE_NM"])
        
        # sftp 및 ssh 닫기
        sftp_client.close()
        ssh_client.close()
        
    except Exception as e:
        # 상태코드 변경(파일삭제 에러)
        data["STATE"] = "0611"
        data["MESSAGE"] = "파일삭제중 알 수 없는 오류가 발생했습니다. %s"%e
        print(data["MESSAGE"])
        change_state_code(data["SEQ_NO"], data["STATE"])
        return 'task_error'

    # 상태코드 변경(파일전송 시작)
    data["STATE"] = "0610"
    data["MESSAGE"] = "파일삭제 완료."
    change_state_code(data["SEQ_NO"], data["STATE"])
    kwargs['ti'].xcom_push(key='data', value=data)

    return 'complete'

# PRCS_CD 변경
def change_state_code(seq_no, prcs_cd):
    # mysql 접속
    connection = pymysql.connect(host=str(os.getenv("mysql_host")), 
                                user=str(os.getenv("mysql_user")), 
                                password=str(os.getenv("mysql_password")), 
                                db=str(os.getenv("mysql_db")), 
                                charset='utf8')
    cursor = connection.cursor()
    query = "update TB_TAGG_HIST_copy set PRCS_CD=%s where SEQ_NO=%s;"
    cursor.execute(query, (prcs_cd, seq_no))
    
    # 연결종료
    connection.commit()
    connection.close()


if __name__ == '__main__':
    
    ## test
    data = connect_mysql_return_values()
    data = preprocessing(data)
    data = text_classification(data)

    print(data)
    
