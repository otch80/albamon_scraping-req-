# DB
from sqlalchemy import create_engine
import pymysql, sqlalchemy
from sqlalchemy.dialects import mysql
import pandas as pd

import job_code

user_info = {
    "username" : "",
    "password" : "", # 입력
    "database" : "", # 입력
}

class DB:
    def __init__(self):
        self.connect_db()

    def connect_db(self):
        print(">>> Initializing Connection Settings")
        pymysql.install_as_MySQLdb()
        self.engine = create_engine("mysql+mysqldb://root:" + user_info['password'] + "@localhost/" + user_info['database'] + "?charset=utf8mb4", encoding = 'utf8')
        self.conn = self.engine.connect()
        print(">>> Connecting Success")

    def create_posting_table(self):
        try:
            try:
                from sqlalchemy import Table, Column, MetaData
                metadata = MetaData()
            except:
                print(">>> sqlalchemy is not founded. Will you install sqlalchemy? [yes/no]: ")
                answer = input()
                if (answer == 'y' or answer == 'Y' or answer == 'yes' or answer == 'YES' or answer == 'Yes'):
                    import os
                    os.system('pip install sqlalchemy')
                else:
                    raise Exception(">>> Package is not founed. Can't proceed any further.")
            try:
                temp_table = Table('user_jobposting', metadata,
                                   # Column('id', mysql.INTEGER),                               # 인덱스
                                   Column('city', mysql.VARCHAR(80)),  # 근무지 (광역시, 도)
                                   Column('county', mysql.VARCHAR(80)),  # 근무지 (시군구)
                                   Column('company', mysql.VARCHAR(80), nullable=False),  # 상호명
                                   Column('subtitle', mysql.VARCHAR(200)),  # 기간
                                   Column('pay', mysql.INTEGER, nullable=False),  # 급여
                                   Column('pay_type', mysql.VARCHAR(8), nullable=False),  # 지급형태
                                   Column('url', mysql.VARCHAR(100), nullable=False),  # url
                                   Column('sub_code', mysql.VARCHAR(8)),  # 하위코드명
                                   Column('enrol_date', mysql.DATE())  # 공고등록일
                                   )
                temp_table.create(self.engine)  # create the table
            except Exception as e:
                print(">>> user_jobposting Table is already exists")
            del [[temp_table]]
        except:
            return False

    def create_log_table(self):
        from sqlalchemy import Table, Column, MetaData, DateTime
        metadata = MetaData()
        log_table = Table("scrap_log", metadata,
                  Column('1000', mysql.INTEGER),
                  Column('2000', mysql.INTEGER),
                  Column('3000', mysql.INTEGER),
                  Column('4000', mysql.INTEGER),
                  Column('6000', mysql.INTEGER),
                  Column('7000', mysql.INTEGER),
                  Column('8000', mysql.INTEGER),
                  Column('9000', mysql.INTEGER),
                  Column('A000', mysql.INTEGER),
                  Column('B000', mysql.INTEGER),
                  Column('C000', mysql.INTEGER),
                  Column('D000', mysql.INTEGER),
                  Column('E000', mysql.INTEGER),
                  Column('total_cnt', mysql.INTEGER),
                  Column('run_time', mysql.INTEGER, nullable=False),
                  Column('date', DateTime, nullable=False, primary_key=True)
        )
        try:
            log_table.create(self.engine)  # create the table
        except:
            print(">>> Job Log Table is already exists")

        del [[log_table]]


    def insert_table(self, job_df, run_time, today) :

        log = {'1000': 0, '2000': 0, '3000': 0, '4000': 0, '6000': 0, '7000': 0, '8000': 0, '9000': 0, 'A000': 0,
                 'B000': 0, 'C000': 0, 'D000': 0, 'E000': 0}

        for i in range(len(job_df)):
            log[job_code.match_job_code(job_df['sub_code'][i])] += 1

        log['1000'] = [log['1000']]
        log['2000'] = [log['2000']]
        log['3000'] = [log['3000']]
        log['4000'] = [log['4000']]
        log['6000'] = [log['6000']]
        log['7000'] = [log['7000']]
        log['8000'] = [log['8000']]
        log['9000'] = [log['9000']]
        log['A000'] = [log['A000']]
        log['B000'] = [log['B000']]
        log['C000'] = [log['C000']]
        log['D000'] = [log['D000']]
        log['E000'] = [log['E000']]
        log['total_cnt'] = len(job_df)
        log['run_time'] = run_time
        log['date'] = today

        log_df = pd.DataFrame(log)

        job_df.to_sql(name="user_jobposting", con=self.engine, if_exists='append', index=False)
        log_df.to_sql(name="scrap_log", con=self.engine, if_exists='append', index=False)

        print(">>> Table Insert Complete")