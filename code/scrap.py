from urllib.request import urlopen
from urllib import parse
from bs4 import BeautifulSoup
from math import *
from datetime import datetime
from job_code import job_code
from tqdm import tqdm

import os, time, re, random, main
import pandas as pd

class Scrap:
    def __init__(self):
        self.job_code = job_code
        print(">>> job code 로드 완료")

        self.crawl_mon = main.crawl_mon()
        print(">>> Main 객체 생성 완료")

        self.df = self.scrap(self.job_code)
        print(">>> 스크랩 완료")

    # 데이터 추출 및 데이터프레임 변환
    def scrap(self, task_list):
        day = datetime.today().strftime("%Y-%m-%d")
        result_df = pd.DataFrame(
            columns=['city', 'county', 'company', 'subtitle', 'url', 'gender', 'age', 'pay_type', 'pay', 'sub_code',
                     'enrol_date']
        )

        result_list = []

        # 보안문자 확인
        warnSec = "보안문자를 입력해 주시기 바랍니다"
        pSec = re.compile(warnSec)

        # 성인인증
        warnAdult = "teenLoginFom"
        pAd = re.compile(warnAdult)

        # 상위 카테고리
        for title, title_code in tqdm(dict(task_list).items(), desc='전체'):
            # 하위 카테고리
            for sub_title, sub_title_code in tqdm(title_code.items(), desc='하위코드', mininterval=0.05):

                # 총 게시물 수를 통한 전체 페이지 갯수 확인
                url = self.crawl_mon.make_url(sub_title_code, page=1)
                parsed = parse.urlparse(url)  # 공고 url 생성용
                page = urlopen(url)
                bs = BeautifulSoup(page, features="html.parser")

                # 보안문자
                if pSec.search(bs.text):
                    print(">>> [Error] 보안문자가 발생하였습니다")
                    print(">>> url : ", url)
                    return False
                # 성인인증
                if pAd.search(bs):
                    print(">>> [Error] {} 성인인증 오류 발생".format(sub_title))
                    continue
                
                # 초기화
                totalCount = bs.find("div", "pageSubTit").find("em").text

                r = re.findall("[0-9]", totalCount)
                PagesPerData = int(''.join(r))
                Pages = ceil(PagesPerData / 50)  # ceil 올림함수

                # url 리스트 생성
                urls = [self.crawl_mon.make_url(sub_title_code, rWDate=4, ps=50, page=i) for i in range(1, Pages + 1)]

                for url in urls:
                    # 파싱

                    # 랜덤 시간을 통한 req (매크로 방지용)
                    randomTime = random.randrange(5, 10)  # 5 ~ 10초 간격으로 req
                    time.sleep(randomTime)

                    parsed = parse.urlparse(url)
                    page = urlopen(url)
                    bs = BeautifulSoup(page, features="html.parser")

                    # 보안문자 발생시 종료
                    if pSec.search(bs.text):
                        print(">>> [Error] 보안문자가 발생하였습니다")
                        print(">>> url : ", url)
                        return False
                    if pAd.search(bs):
                        print(">>> [Error] {} 성인인증 오류 발생".format(sub_title))
                        break

                    tbody = bs.select("tbody")
                    trs = tbody[len(tbody) - 1].find_all("tr")

                    for tr in trs:
                        temp_list = []
                        # 'city', 'county'
                        try:
                            area = \
                            tr.find(name="td", attrs="area").find_all(name="div")[0].text.split("스크랩\n")[1].split('\n')[
                                0]

                            city = area.split(" ")[0]

                            county = area.split(" ")[1]
                        except:
                            city = ''
                            county = ''

                        # company.
                        try:
                            company = tr.find(name="td", attrs="subject").find_all(name="p", attrs={"cName"})[0].text
                        except:
                            company = ""

                        # subtitle.
                        try:
                            subtitle = tr.find(name="td", attrs="subject").find_all(name="p", attrs={"cTit"})[0].text
                        except:
                            subtitle = ""

                        # url
                        try:
                            url = tr.find("a").get("href")
                            url = parsed.scheme + "://" + parsed.netloc + url
                        except:
                            url = ""

                        # gender.
                        try:
                            gender = tr.find(name="p", attrs={"gender"}).text
                        except:
                            gender = "무관"

                        # age.
                        try:
                            age = tr.find(name="p", attrs={"age"}).text
                        except:
                            age = "무관"

                        # pay_type.
                        try:
                            pay_type = tr.find(name="td", attrs={"pay"}).find("img").get("alt")
                        except:
                            pay_type = ""

                        # pay.
                        try:
                            temp_pay = tr.find(name="td", attrs={"pay"}).find_all("p")[1].text
                            r_pay = re.findall("[0-9]", temp_pay)
                            pay = int(''.join(r_pay))
                        except:
                            pay = ""

                        result_list.append(
                            [city, county, company, subtitle, url, gender, age, pay_type, pay, sub_title_code, day])
        result_df = pd.DataFrame(result_list,
                                 columns=['city', 'county', 'company', 'subtitle', 'url', 'gender', 'age', 'pay_type',
                                          'pay', 'sub_code', 'enrol_date'])
        result_df = result_df.dropna(axis=0)
        return result_df