from datetime import datetime
import re, time
import pandas as pd
import os
import db
import multy_scrap
import scrap


class crawl_mon:
    def __init__(self):
        self.warn = "보안문자를 입력해 주시기 바랍니다"
        self.pattern = re.compile(self.warn)
        self.day = datetime.today().strftime("%Y-%m-%d")

        path = "./log"
        if not os.path.isdir(path):
            os.mkdir(path)

    def make_url(self, code, rWDate=4, ps=50, page=1):
        url = "https://www.albamon.com/list/gi/mon_part_list.asp?"

        url += "rpcd=" + str(code) + "&rWDate=" + str(rWDate) + "&ps=" + str(ps) + "&page=" + str(page)
        return url


if __name__ == "__main__":
    start = time.time()

    # rw = 4 # 당일 등록된 공고만
    rw = 3 # 3일 이내 등록된 공고만
    # crawl = multy_scrap.MultyScrap()
    crawl = scrap.Scrap(rw)

    if crawl == False:
        print("[Error] 코드 에러")
        exit()
    day = datetime.today().strftime("%Y-%m-%d")


    crawl.df.to_csv("./log/" + day + ".csv", index=False, encoding="utf-8-sig")
    end = time.time() - start
    print(">>> Scrap time : ", end)
    print(f">>> 수집 데이터 : {crawl.df.shape}")

    # df = pd.read_csv("./log/2021-11-14.csv")

    start = time.time()
    day = datetime.today()
    end = time.time() - start

    db = db.DB()
    db.create_posting_table()
    db.create_log_table()

    # db.insert_table(crawl.df, end, day)
    db.insert_table(df, end, day)
