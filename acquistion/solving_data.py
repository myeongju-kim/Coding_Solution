import requests
import urllib.request
from bs4 import BeautifulSoup
import csv
import time

# 로그인 쿠키 저장
headers = {'User-Agent': 'Mozilla/5.0'}
session = requests.session()

login_info = {
    "login_user_id": "mjoo1106@naver.com",
    "login_password": "mjoo08520130!"
}
url_login = "https://www.acmicpc.net/signin"
res = session.post(url_login, data = login_info)
print(res.raise_for_status())
html=requests.get("https://www.acmicpc.net/modify",headers=headers).text
soup=BeautifulSoup(html,'html.parser')



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

html = requests.get('https://www.acmicpc.net/status?top=43115731', headers=headers).text
soup = BeautifulSoup(html, "html.parser")
nid = "solution-43115731"
tr = soup.find(id=nid)
tds = tr.find_all('td')




'''
filename='test.csv'
f=open(filename, 'w',encoding='utf-8-sig', newline='')
list_title=['사용자티어','사용자','난이도','문제번호','제목','분류', '정답', '메모리', '시간', '언어','코드길이','제출날짜']

writer=csv.writer(f)
writer.writerow(list_title)

start=43115731
while start>=38000000:
    temp=start
    html = requests.get('https://www.acmicpc.net/status?top=' + str(temp), headers=headers).text
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
        print(tds[2])
        #사용자 정보 tds[1]
        nick=tds[1].text
        htmlu = requests.get('https://www.acmicpc.net/user/' + nick, headers=headers).text
        soupu = BeautifulSoup(htmlu, "html.parser")
        try:
            u_tier = soupu.find("img", "solvedac-tier")
            index = u_tier["src"].split("/")[4].split(".")[0]
            rows.append(tier_map[index])
        except:
            rows.append("UNRANK")

        rows.append(nick)

        #문제 정보 tds[2]
        try:
            q_tier = tds[2].find("img", "solvedac-tier")
            index=q_tier["src"].split("/")[4].split(".")[0]
            rows.append(tier_map[index])
        except:
            rows.append("UNRANK")
        que=tds[2].find("a")
        rows.append(que.text)
        rows.append(que['title'])

        #들어가서 정보 수집
        htmlq = requests.get('https://www.acmicpc.net/problem/' + str(que.text), headers=headers).text
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

        for index in range(3,8):
            rows.append(tds[index].text.strip())
        rows.append(tds[8].find('a')['title'])
        writer.writerow(rows)
        print(rows)
        start -= 1
'''