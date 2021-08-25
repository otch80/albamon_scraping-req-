from datetime import datetime
import re, time
import pandas as pd
import db
import multy_scrap
import scrap


class crawl_mon:
    def __init__(self):
        self.warn = "보안문자를 입력해 주시기 바랍니다"
        self.pattern = re.compile(self.warn)
        self.day = datetime.today().strftime("%Y-%m-%d")

        self.rWDate = 4  # 오늘 등록된 게시물만 보기 (get element)
        self.ps = 50  # 최대 노출 게시물 수

    def make_url(self, code, rWDate=4, ps=50, page=1):
        url = "https://www.albamon.com/list/gi/mon_part_list.asp?"
        url += "rpcd=" + str(code) + "&rWDate=" + str(rWDate) + "&ps=" + str(ps) + "&page=" + str(page)
        return url

    def divide_mbti(self, df):
        # print("df : ",df.shape)
        df.sub_code = df.sub_code.astype(str)
        mbti_df = pd.read_csv("../MBTI list.csv")
        columns_list = ['city', 'county', 'company', 'pay', 'pay_type', 'gender', 'age', 'url', 'subtitle', 'sub_code',
                     'star', 'enrol_date']

        self.ISTJ = pd.DataFrame(columns=columns_list)
        self.ISFJ = pd.DataFrame(columns=columns_list)
        self.INFJ = pd.DataFrame(columns=columns_list)
        self.INTJ = pd.DataFrame(columns=columns_list)
        self.ISTP = pd.DataFrame(columns=columns_list)
        self.ISFP = pd.DataFrame(columns=columns_list)
        self.INFP = pd.DataFrame(columns=columns_list)
        self.INTP = pd.DataFrame(columns=columns_list)
        self.ESTP = pd.DataFrame(columns=columns_list)
        self.ESFP = pd.DataFrame(columns=columns_list)
        self.ENFP = pd.DataFrame(columns=columns_list)
        self.ENTP = pd.DataFrame(columns=columns_list)
        self.ESTJ = pd.DataFrame(columns=columns_list)
        self.ESFJ = pd.DataFrame(columns=columns_list)
        self.ENFJ = pd.DataFrame(columns=columns_list)
        self.ENTJ = pd.DataFrame(columns=columns_list)

        # sub_code별 mbti 만들기
        mbti_dict = dict()
        for group in mbti_df.groupby('직종코드'):
            mbti_dict[group[0]] = group[1]['mbti']

        # mbti별 공고 분류
        for key, value in mbti_dict.items():
            for mbti in value:
                if (mbti == "ISTJ"):
                    self.ISTJ = pd.concat([self.ISTJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ISFJ"):
                    self.ISFJ = pd.concat([self.ISFJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "INFJ"):
                    self.INFJ = pd.concat([self.INFJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "INTJ"):
                    self.INTJ = pd.concat([self.INTJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ISTP"):
                    self.ISTP = pd.concat([self.ISTP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ISFP"):
                    self.ISFP = pd.concat([self.ISFP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "INFP"):
                    self.INFP = pd.concat([self.INFP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "INTP"):
                    self.INTP = pd.concat([self.INTP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ESTP"):
                    self.ESTP = pd.concat([self.ESTP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ESFP"):
                    self.ESFP = pd.concat([self.ESFP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ENFP"):
                    self.ENFP = pd.concat([self.ENFP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ENTP"):
                    self.ENTP = pd.concat([self.ENTP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ESTJ"):
                    self.ESTJ = pd.concat([self.ESTJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ESFJ"):
                    self.ESFJ = pd.concat([self.ESFJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ENFJ"):
                    self.ENFJ = pd.concat([self.ENFJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ENTJ"):
                    self.ENTJ = pd.concat([self.ENTJ, df.loc[df['sub_code'] == key]], sort=False)

    def divide_jobver(self, df):
        columns_list = ['city', 'county', 'company', 'pay', 'pay_type', 'gender', 'age', 'url', 'subtitle', 'sub_code',
                        'star', 'enrol_date']

        self.food = pd.DataFrame(columns=columns_list)
        self.sale = pd.DataFrame(columns=columns_list)
        self.cult = pd.DataFrame(columns=columns_list)
        self.serv = pd.DataFrame(columns=columns_list)
        self.desk = pd.DataFrame(columns=columns_list)
        self.rsch = pd.DataFrame(columns=columns_list)
        self.buil = pd.DataFrame(columns=columns_list)
        self.comp = pd.DataFrame(columns=columns_list)
        self.edct = pd.DataFrame(columns=columns_list)
        self.desg = pd.DataFrame(columns=columns_list)
        self.medi = pd.DataFrame(columns=columns_list)
        self.deli = pd.DataFrame(columns=columns_list)
        self.oper = pd.DataFrame(columns=columns_list)

        self.food = pd.concat([self.food, df.loc[df['sub_code'].str.startswith("1")]], sort=False)
        self.sale = pd.concat([self.sale, df.loc[df['sub_code'].str.startswith("2")]], sort=False)
        self.cult = pd.concat([self.cult, df.loc[df['sub_code'].str.startswith("3")]], sort=False)
        self.serv = pd.concat([self.serv, df.loc[df['sub_code'].str.startswith("4")]], sort=False)
        self.desk = pd.concat([self.desk, df.loc[df['sub_code'].str.startswith("6")]], sort=False)
        self.rsch = pd.concat([self.rsch, df.loc[df['sub_code'].str.startswith("7")]], sort=False)
        self.buil = pd.concat([self.buil, df.loc[df['sub_code'].str.startswith("8")]], sort=False)
        self.comp = pd.concat([self.comp, df.loc[df['sub_code'].str.startswith("9")]], sort=False)
        self.edct = pd.concat([self.edct, df.loc[df['sub_code'].str.startswith("A")]], sort=False)
        self.desg = pd.concat([self.desg, df.loc[df['sub_code'].str.startswith("B")]], sort=False)
        self.medi = pd.concat([self.medi, df.loc[df['sub_code'].str.startswith("C")]], sort=False)
        self.deli = pd.concat([self.deli, df.loc[df['sub_code'].str.startswith("D")]], sort=False)
        self.oper = pd.concat([self.oper, df.loc[df['sub_code'].str.startswith("E")]], sort=False)


if __name__ == "__main__":
    # start = time.time()
    # crawl = multy_scrap.MultyScrap()
    # # crawl = scrap.Scrap()
    #
    # if crawl == False:
    #     print("[Error] 코드 에러")
    #     exit()
    # day = datetime.today().strftime("%Y-%m-%d")
    # crawl.df.to_csv("../log/" + day + ".csv", index=False, encoding="utf-8-sig")
    # end = time.time() - start
    # print(">>> Scrap time : ", end)



    start = time.time()
    day = datetime.today().strftime("%Y-%m-%d")
    end = time.time() - start

    mon = crawl_mon()
    mon.divide_mbti(pd.read_csv("../log/2021-08-24.csv"))
    # mon.divide_mbti(crawl.df)

    db = db.DB()
    db.create_table()
    db.create_log_table()

    db.insert_table(mon.ISTJ, mon.ISFJ, mon.INFJ, mon.INTJ, mon.ISTP, mon.ISFP, mon.INFP, mon.INTP, mon.ESTP, mon.ESFP,
                    mon.ENFP, mon.ENTP, mon.ESTJ, mon.ESFJ, mon.ENFJ, mon.ENTJ, end, day)

    db.insert_total_data(crawl.df)


    # job_version

    # mon.divide_jobver(pd.read_csv("../log/2021-08-24.csv"))
    #
    # db = db.DB()
    # db.create_table_jobver()
    # db.create_log_table_jobver()
    #
    # db.insert_table_jobver(mon.food, mon.sale, mon.cult, mon.serv, mon.desk, mon.rsch, mon.buil,
    #                        mon.comp, mon.edct, mon.desg, mon.medi, mon.deli, mon.oper, end, day)
    #
    # db.insert_total_data(crawl.df)