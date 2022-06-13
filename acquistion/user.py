import requests
from bs4 import BeautifulSoup
import json

headers = {'User-Agent': 'Mozilla/5.0'}
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
dict={}
num=1
for i in range(261,1001):
    html = requests.get('https://www.acmicpc.net/ranklist/'+str(i), headers=headers).text
    soup=BeautifulSoup(html,'html.parser')
    rank=soup.find(id="ranklist")
    trs=rank.find_all('tr')
    trs.pop(0)
    for tr in trs:
        tds=tr.find_all('td')
        user=tds[1].text
        uhtml = requests.get('https://www.acmicpc.net/user/' + user, headers=headers).text
        usoup = BeautifulSoup(uhtml, 'html.parser')
        tier=""
        try:
            t=usoup.find("img","solvedac-tier")
            index = t["src"].split("/")[4].split(".")[0]
            tier=tier_map[index]
        except:
            tier="UNRANK"
        dict[tds[1].text]=tier
        num+=1
        print(num)
        if num%1000==0:
            with open("users3.json", 'w', encoding='utf-8') as file:
                json.dump(dict, file, indent="\t", ensure_ascii=False)

with open("users3.json", 'w', encoding='utf-8') as file:
    json.dump(dict, file,indent="\t", ensure_ascii=False)
