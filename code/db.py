# DB
from sqlalchemy import create_engine
import pymysql, sqlalchemy
from sqlalchemy.dialects import mysql

import pandas as pd

user_info = {
    "username" : "root",
    "password" : "!whals7628rb1", # 입력
    "database" : "capstone", # 입력
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


    def create_mbti_table(self):
        try:
            from sqlalchemy import Table, Column, MetaData
            metadata = MetaData()
        except:
            print(">>> sqlalchemy is not founded. Will you install sqlalchemy? [yes/no]: ")
            answer = input()
            if(answer == 'y' or answer == 'Y' or answer == 'yes' or answer == 'YES' or answer == 'Yes'):
                import os
                os.system('pip install sqlalchemy')
            else:
                raise Exception(">>> Package is not founed. Can't proceed any further.")
        try:
            self.MBTI_list = ['istj', 'isfj', 'infj', 'intj', 'istp', 'isfp', 'infp', 'intp', 'estp', 'esfp', 'enfp', 'entp', 'estj', 'esfj', 'enfj', 'entj']

            for mbti in self.MBTI_list:
                temp_table = Table(mbti, metadata,
                   # Column('id', mysql.INTEGER),                               # 인덱스
                   Column('city', mysql.VARCHAR(80)),                           # 근무지 (광역시, 도)
                   Column('county', mysql.VARCHAR(80)),                         # 근무지 (시군구)
                   Column('company', mysql.VARCHAR(80), nullable=False),        # 상호명
                   Column('pay', mysql.INTEGER, nullable=False),                # 급여
                   Column('pay_type', mysql.VARCHAR(8), nullable=False),        # 지급형태
                #    Column('gender', mysql.VARCHAR(8)),                          # 성별
                #    Column('age', mysql.INTEGER),                                # 나이
                   Column('url', mysql.VARCHAR(100), nullable=False),           # url
                   Column('subtitle', mysql.VARCHAR(200)),                      # 기간
                   Column('sub_code', mysql.VARCHAR(8)),                        # 하위코드명
                   Column('enrol_date', mysql.DATE())                           # 공고등록일
                )
                try:
                    temp_table.create(self.engine)  # create the table
                except Exception as e:
                    print(">>> {} Table is already exists".format(mbti))

                del[[temp_table]]
            return True
        except:
            return False


    def create_log_table(self):
        from sqlalchemy import Table, Column, MetaData, DateTime
        metadata = MetaData()

        log_table = Table("mbtilog", metadata,
                Column('istj', mysql.INTEGER),
                Column('isfj', mysql.INTEGER),
                Column('infj', mysql.INTEGER),
                Column('intj', mysql.INTEGER),
                Column('istp', mysql.INTEGER),
                Column('isfp', mysql.INTEGER),
                Column('infp', mysql.INTEGER),
                Column('intp', mysql.INTEGER),
                Column('estp', mysql.INTEGER),
                Column('esfp', mysql.INTEGER),
                Column('enfp', mysql.INTEGER),
                Column('entp', mysql.INTEGER),
                Column('estj', mysql.INTEGER),
                Column('esfj', mysql.INTEGER),
                Column('enfj', mysql.INTEGER),
                Column('entj', mysql.INTEGER),
                Column('total_cnt', mysql.INTEGER),
                Column('run_time', mysql.INTEGER, nullable=False),
                Column('date', DateTime, nullable=False, primary_key=True)
        )
        try:
            log_table.create(self.engine)  # create the table
        except:
            print(">>> Log Table is already exists")

        del [[log_table]]


    def insert_mbti_table(self, ISTJ_df, ISFJ_df, INFJ_df, INTJ_df, ISTP_df, ISFP_df, INFP_df, INTP_df, ESTP_df, ESFP_df, ENFP_df, ENTP_df, ESTJ_df, ESFJ_df, ENFJ_df, ENTJ_df, rum_time, today):

        # log Table
        log_columns = ['ISTJ','ISFJ','INFJ','INTJ','ISTP','ISFP','INFP','INTP','ESTP','ESFP','ENFP','ENTP','ESTJ','ESFJ','ENFJ','ENTJ','total_cnt','run_time','date']

        sum = ISTJ_df.shape[0] + ISFJ_df.shape[0] + INFJ_df.shape[0] + INTJ_df.shape[0] + ISTP_df.shape[0] + \
              ISFP_df.shape[0] + INFP_df.shape[0] + INTP_df.shape[0] + ESTP_df.shape[0] + ESFP_df.shape[0] + \
              ENFP_df.shape[0] + ENTP_df.shape[0] + ESTJ_df.shape[0] + ESFJ_df.shape[0] + ENFJ_df.shape[0] + \
              ENTJ_df.shape[0]

        log_df = pd.DataFrame(
            [(ISTJ_df.shape[0], ISFJ_df.shape[0], INFJ_df.shape[0], INTJ_df.shape[0], ISTP_df.shape[0],
              ISFP_df.shape[0], INFP_df.shape[0], INTP_df.shape[0], ESTP_df.shape[0], ESFP_df.shape[0],
              ENFP_df.shape[0], ENTP_df.shape[0], ESTJ_df.shape[0], ESFJ_df.shape[0], ENFJ_df.shape[0],
              ENTJ_df.shape[0], sum, rum_time, today)], columns=log_columns)


        ISTJ_df.to_sql(name="istj", con=self.engine, if_exists='append', index=False)
        ISFJ_df.to_sql(name="isfj", con=self.engine, if_exists='append', index=False)
        INFJ_df.to_sql(name="infj", con=self.engine, if_exists='append', index=False)
        INTJ_df.to_sql(name="intj", con=self.engine, if_exists='append', index=False)
        ISTP_df.to_sql(name="istp", con=self.engine, if_exists='append', index=False)
        ISFP_df.to_sql(name="isfp", con=self.engine, if_exists='append', index=False)
        INFP_df.to_sql(name="infp", con=self.engine, if_exists='append', index=False)
        INTP_df.to_sql(name="intp", con=self.engine, if_exists='append', index=False)
        ESTP_df.to_sql(name="estp", con=self.engine, if_exists='append', index=False)
        ESFP_df.to_sql(name="esfp", con=self.engine, if_exists='append', index=False)
        ENFP_df.to_sql(name="enfp", con=self.engine, if_exists='append', index=False)
        ENTP_df.to_sql(name="entp", con=self.engine, if_exists='append', index=False)
        ESTJ_df.to_sql(name="estj", con=self.engine, if_exists='append', index=False)
        ESFJ_df.to_sql(name="esfj", con=self.engine, if_exists='append', index=False)
        ENFJ_df.to_sql(name="enfj", con=self.engine, if_exists='append', index=False)
        ENTJ_df.to_sql(name="entj", con=self.engine, if_exists='append', index=False)
        log_df.to_sql(name="mbtilog", con=self.engine, if_exists='append', index=False)

        print(">>> MBTI Table Insert Complete")      



    # _jobver : 수집된 공고를 직종별로 DB에 분류

    def create_job_table(self):
        try:
            from sqlalchemy import Table, Column, MetaData
            metadata = MetaData()
        except:
            print(">>> sqlalchemy is not founded. Will you install sqlalchemy? [yes/no]: ")
            answer = input()
            if(answer == 'y' or answer == 'Y' or answer == 'yes' or answer == 'YES' or answer == 'Yes'):
                import os
                os.system('pip install sqlalchemy')
            else:
                raise Exception(">>> Package is not founed. Can't proceed any further.")
        try:
            self.job_list = {'food', 'sale', 'cult', 'serv', 'desk', 'rsch', 'buil', 'comp', 'edct', 'desg', 'medi', 'deli', 'oper'}
            for job_code in self.job_list:
                # temp_table = Table(job_code, metadata,
                #     # Column('id', mysql.INTEGER),                          # 인덱스
                #     Column('region', mysql.VARCHAR(20), nullable=False),    # 지역
                #     Column('B_name', mysql.VARCHAR(80), nullable=False),    # 상호명
                #     Column('pay', mysql.INTEGER, nullable=False),           # 급여
                #     Column('pay_type', mysql.VARCHAR(8), nullable=False),   # 지급형태
                #     Column('city', mysql.VARCHAR(80)),                      # 근무지 (광역시, 도)
                #     Column('county', mysql.VARCHAR(80)),                    # 근무지 (시군구)
                #     Column('working_time', mysql.VARCHAR(20)),              # 근무시간
                #     Column('url', mysql.VARCHAR(100), nullable=False),      # url
                #     Column('working_period', mysql.VARCHAR(36)),            # 기간
                #     Column('day', mysql.VARCHAR(20)),                       # 요일
                #     Column('sub_code', mysql.VARCHAR(8)),                   # 하위코드명
                #     Column('enrol_date', mysql.DATE())                      # 공고등록일
                # )
                temp_table = Table(job_code, metadata,
                   # Column('id', mysql.INTEGER),                               # 인덱스
                   Column('city', mysql.VARCHAR(80)),                           # 근무지 (광역시, 도)
                   Column('county', mysql.VARCHAR(80)),                         # 근무지 (시군구)
                   Column('company', mysql.VARCHAR(80), nullable=False),        # 상호명
                   Column('pay', mysql.INTEGER, nullable=False),                # 급여
                   Column('pay_type', mysql.VARCHAR(8), nullable=False),        # 지급형태
                #    Column('gender', mysql.VARCHAR(8)),                          # 성별
                #    Column('age', mysql.INTEGER),                                # 나이
                   Column('url', mysql.VARCHAR(100), nullable=False),           # url
                   Column('subtitle', mysql.VARCHAR(200)),                      # 기간
                   Column('sub_code', mysql.VARCHAR(8)),                        # 하위코드명
                   Column('enrol_date', mysql.DATE())                           # 공고등록일
                )

                try:
                    temp_table.create(self.engine)  # create the table
                except Exception as e:
                    print(">>> {} Table is already exists".format(job_code))
                del[[temp_table]]
            return True
        except:
            return False


    def create_joblog_table(self):
        from sqlalchemy import Table, Column, MetaData, DateTime
        metadata = MetaData()
        log_table = Table("joblog", metadata,
                Column('food', mysql.INTEGER),
                Column('sale', mysql.INTEGER),
                Column('cult', mysql.INTEGER),
                Column('serv', mysql.INTEGER),
                Column('desk', mysql.INTEGER),
                Column('rsch', mysql.INTEGER),
                Column('buil', mysql.INTEGER),
                Column('comp', mysql.INTEGER),
                Column('edct', mysql.INTEGER),
                Column('desg', mysql.INTEGER),
                Column('medi', mysql.INTEGER),
                Column('deli', mysql.INTEGER),
                Column('oper', mysql.INTEGER),
                Column('total_cnt', mysql.INTEGER),
                Column('run_time', mysql.INTEGER, nullable=False),
                Column('date', DateTime, nullable=False, primary_key=True)
        )
        try:
            log_table.create(self.engine)  # create the table
        except:
            print(">>> Job Log Table is already exists")

        del [[log_table]]


    def insert_job_table(self, food_df, sale_df, cult_df, serv_df, desk_df, rsch_df, buil_df, comp_df, edct_df, desg_df, medi_df, deli_df, oper_df, rum_time, today) :

        # log Table
        log_columns = ['food', 'sale', 'cult', 'serv', 'desk', 'rsch', 'buil', 'comp', 'edct', 'desg', 'medi', 'deli', 'oper','total_cnt','run_time','date']

        sum = food_df.shape[0] + sale_df.shape[0] + cult_df.shape[0] + serv_df.shape[0] + desk_df.shape[0] + \
                rsch_df.shape[0] + buil_df.shape[0] + comp_df.shape[0] + edct_df.shape[0] + desg_df.shape[0] + \
                medi_df.shape[0] + deli_df.shape[0] + oper_df.shape[0]

        log_df = pd.DataFrame(
            [(food_df.shape[0], sale_df.shape[0], cult_df.shape[0], serv_df.shape[0], desk_df.shape[0],
              rsch_df.shape[0], buil_df.shape[0], comp_df.shape[0], edct_df.shape[0], desg_df.shape[0],
              medi_df.shape[0], deli_df.shape[0], oper_df.shape[0], sum, rum_time, today)],
            columns=log_columns)


        food_df.to_sql(name="food", con=self.engine, if_exists='append', index=False)
        sale_df.to_sql(name="sale", con=self.engine, if_exists='append', index=False)
        cult_df.to_sql(name="cult", con=self.engine, if_exists='append', index=False)
        serv_df.to_sql(name="serv", con=self.engine, if_exists='append', index=False)
        desk_df.to_sql(name="desk", con=self.engine, if_exists='append', index=False)
        rsch_df.to_sql(name="rsch", con=self.engine, if_exists='append', index=False)
        buil_df.to_sql(name="buil", con=self.engine, if_exists='append', index=False)
        comp_df.to_sql(name="comp", con=self.engine, if_exists='append', index=False)
        edct_df.to_sql(name="edct", con=self.engine, if_exists='append', index=False)
        desg_df.to_sql(name="desg", con=self.engine, if_exists='append', index=False)
        medi_df.to_sql(name="medi", con=self.engine, if_exists='append', index=False)
        deli_df.to_sql(name="deli", con=self.engine, if_exists='append', index=False)
        oper_df.to_sql(name="oper", con=self.engine, if_exists='append', index=False)
        log_df.to_sql(name="joblog", con=self.engine, if_exists='append', index=False)

        print(">>> Table Insert Complete")