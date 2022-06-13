from selenium import webdriver
from bs4 import BeautifulSoup
import csv
import time


tier_map={
    "0":"UNRANK",
    "1":"브론즈 5",
    "2":"브론즈 4",
    "3":"브론즈 3",
    "4":"브론즈 2",
    "5":"브론즈 1",
    "6":"실버 5",
    "7":"실버 4",
    "8":"실버 3",
    "9":"실버 2",
    "10":"실버 1",
    "11":"골드 5",
    "12":"골드 4",
    "13":"골드 3",
    "14":"골드 2",
    "15":"골드 1",
    "16":"플래티넘 5",
    "17":"플래티넘 4",
    "18":"플래티넘 3",
    "19":"플래티넘 2",
    "20":"플래티넘 1",
    "21":"다이아몬드 5",
    "22":"다이아몬드 4",
    "23":"다이아몬드 3",
    "24":"다이아몬드 2",
    "25":"다이아몬드 1",
    "26":"루비 5",
    "27":"루비 4",
    "28":"루비 3",
    "29":"루비 2",
    "30":"루비 1",
    "31":"마스터",
}

filename='test.csv'
f=open(filename, 'w',encoding='utf-8-sig', newline='')
list_title=['사용자티어','사용자','난이도','문제번호','제목','분류', '정답', '메모리', '시간', '언어','코드길이','제출날짜']
writer=csv.writer(f)
writer.writerow(list_title)

driver = webdriver.Chrome('D:\craw\chromedriver.exe')
driver.get('https://www.acmicpc.net/login?next=%2Fstatus%3Ftop%3D43106368')
driver.find_element_by_name('login_user_id').send_keys("your_id")
driver.find_element_by_name('login_password').send_keys("your_pwd")
driver.find_element_by_id('submit_button').click()
time.sleep(1)
start=43115731
while start>=38000000:
    temp=start
    driver.get('https://www.acmicpc.net/status?top=' + str(temp))
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    for i in range(0, 20):
        rows =[]
        nid = "solution-" + str(start)
        tr = soup.find(id=nid)
        tds = ""

        #사용자가 삭제된 경우 예외처리
        try:
            tds=tr.find_all('td')
        except:
            start-=1
            continue

        #사용자 정보 tds[1]
        nick=tds[1].text
        driver.get('https://www.acmicpc.net/user/' + nick)
        htmlu = driver.page_source
        soupu = BeautifulSoup(htmlu, "html.parser")
        try:
            u_tier = soupu.find("img", "solvedac-tier")
            index = u_tier["src"].split("/")[4].split(".")[0]
            rows.append(tier_map[index])
        except:
            rows.append("UNRANK")

        rows.append(nick)
        driver.get('https://www.acmicpc.net/status?top=' + str(temp))

        #문제 정보 tds[2]
        try:
            q_tier = tds[2].find("img", "solvedac-tier")
            index=q_tier["src"].split("/")[4].split(".")[0]
            rows.append(tier_map[index])
        except:
            rows.append("UNRANK")
        que=tds[2].find("a")
        rows.append(que.text)
        rows.append(que['data-original-title'])

        #들어가서 정보 수집
        driver.get('https://www.acmicpc.net/problem/' + str(que.text))
        htmlq = driver.page_source
        soupq = BeautifulSoup(htmlq, "html.parser")
        try:
            algo = ""
            ul=soupq.find('ul','spoiler-list')
            lis=ul.find_all('li')
            for li in lis:
                algo+=li.text.strip()+"/"
            rows.append(algo)
        except:
            rows.append("")
        driver.get('https://www.acmicpc.net/status?top=' + str(temp))
        for index in range(3,8):
            rows.append(tds[index].text.strip())
        rows.append(tds[8].find('a')['data-original-title'])
        writer.writerow(rows)
        print(rows)
        start -= 1




