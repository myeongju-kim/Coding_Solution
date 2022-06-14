from selenium import webdriver
from bs4 import BeautifulSoup
import json
import time
from selenium.webdriver.chrome.options import Options
chrome_options = Options()
chrome_options.headless = True
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
driver = webdriver.Chrome('D:\craw\chromedriver.exe')
driver.get('https://www.acmicpc.net/login?next=%2Fstatus%3Ftop%3D43106368')
driver.find_element_by_name('login_user_id').send_keys("mjoo1106")
driver.find_element_by_name('login_password').send_keys("mjoo08520130!")
driver.find_element_by_id('submit_button').click()
time.sleep(1)
dict={}
for i in range(15501,25165):
    driver.get('https://www.acmicpc.net/problem/'+str(i))
    html=driver.page_source
    soup=BeautifulSoup(html,'html.parser')
    title=""
    try:
        title=soup.find(id="problem_title").text.strip()
    except:
        continue
    tier=""
    try:
        tier = soup.find("img", "solvedac-tier")
        index = tier["src"].split("/")[4].split(".")[0]
        tier=tier_map[index]
    except:
        tier="UNRANK"
    algo=""
    try:
        ul = soup.find('ul', 'spoiler-list')
        lis = ul.find_all('li')
        for li in lis:
            algo += li.text.strip() + "/"
    except:
        algo=""
    dict[str(i)]=[title,tier,algo]
    print(i)
    if i%100==0:
        with open("problem5.json", 'w', encoding='utf-8') as file:
            json.dump(dict, file, indent="\t", ensure_ascii=False)

with open("problem5.json", 'w', encoding='utf-8') as file:
    json.dump(dict, file, indent="\t", ensure_ascii=False)