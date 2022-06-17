<h1 align="center">Prepare for coding test efficiently!<br></h1>
<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
        <li><a href="#introduction">Introduction</a></li>
        <li><a href="#acquisition">Acquisition</a></li>
        <li><a href="#analysis">Analysis</a></li>
        <li><a href="#visualization">Visualization</a></li>
        <li><a href="#demo">Demo</a></li>
  </ol>
</details>


## Introduction
This service targets for **Beginners** who face various difficulites in preparing for a coding test.
It's hard to find information that's right for you at a glance, as troubleshooting sites give you a holistic view. Therefore, we have created a service that users can recognize at a glance by collecting information from problem solving sites.


### Main Functions
1. What types of problems users have solved the most
2. Study structure with people of similar level
3. Pass prediction

### Expectations
Users can learn and develop with other users of a similar level to themselves, and through self-diagnosis, they can figure out what type of problem they make a lot of mistakes. Furthermore, you can even know what the acceptance rate is for a company.

### Install
```
$ git clone -b feature/crawling --single-branch https://github.com/philip-lee-khu/2022-01-PROJECT-GROUP1
$ cd 2022-01-PROJECT-GROUP1
$ pip install -r requirments.txt
$ cd visual
$ npm install
```




## Acquisition
### 1. Getting Started
#### 1.1 Clone the feature/crawling
   ```
    git clone -b feature/crawling --single-branch https://github.com/myeongju-kim/Baekjoon_Analysis
   ```
#### 1.2 Install module
   ```
   pip install -r requirments.txt
   ```
#### 1.3 Run py file!

### 2. Start
Data from websites are acquired using crawling and scraping.
```
# Scraping the user scoring status table at the Baekjun site
url = "https://www.acmicpc.net/status?top=" + str(start)
html = requests.get(url, headers=headers).text
soup = BeautifulSoup(html, 'html.parser')
```

```
# Scraping user, question information from solved.ac
que_url = "https://solved.ac/search?query=" + tds[2].text
que_html = requests.get(que_url, headers=headers).text
que_soup = BeautifulSoup(que_html, 'html.parser')
```
![image](https://user-images.githubusercontent.com/83527620/174279623-ec52a507-7929-47a9-9fa5-9a57a58ad842.png)


## Analysis

### 1. Setup Spark Cluster
This project was based on the Spark 2.4 version running on the Hadoop 2.7 infrastructure.
-  [Install and Setup Cluster Hadoop 2.7.6](https://hadoop.apache.org/docs/r2.7.6/hadoop-project-dist/hadoop-common/ClusterSetup.html)
- [Install and Setup Spark 2.4.8 on Yarn Cluster](https://sparkbyexamples.com/spark/spark-setup-on-hadoop-yarn/)

Please ensure that the Spark Applicaion can be exectued in your environment. 


``` sh
# on master
$ cd {HAOOP_PATH}/sbin
$ ./start-all.sh
This script is Deprecated. Instead use start-dfs.sh and start-yarn.sh
Starting namenodes on [master]
...

$ jps
10000 ResourceManager
10101 SecondaryNameNode
20202 Jps
7777 NameNode

# on slaves
$ jps
10001 NodeManager
10002 Jps
10003 DataNode
```
<br>

### 2. Getting Started

#### 2.1. Getting Started with Jupyter Notebook
You can use the ipynb file in the `analysis/notebook` folder to execute data analysis in the <b>Jupyter Notebook</b> environment.<br>
We recommend that you create a kernel with a virtual environment.
``` sh
$ python -m venv {YOUR_VENV_PATH} # recommended option
$ source {YOUR_VENV_PATH}/bin/activate
$ pip install -r requirements.txt # requirments.txt in 'analysis/notebook'
$ jupyter notebook

...
 Or copy and paste one of these URLs:
        http://localhost:8888/?token={}
...
```
Then you can start analyzing interactively with the ipynb files in the `analysis/notebook` folder.

#### 2.2. Getting Started with spark-submit
If you do not need an interactive processing, you can run the `.py` file under the folder `analysis/script` through the spark-submit command.
``` sh
# Yarn Cluster Mode
$ spark-submit --master yarn [Script].py
# Local Mode
$ spark-submit --master local [Script].py
```

### 3. Functions by each File
- average_problem: Calc difficulty avg by problem type -> problem_avg_difficulty.csv
- monthly_top_algo: Monthly algorithm type submission statistics -> [january ~ may].csv
- predict_acceptance: Build Acceptance Prediction model and Calc Probabilities -> user_with_acceptance_prediction.csv, company_top5_score_criteria.csv
- top_algo_user_and_tiergroup: User and Tiergroup statistics -> algo_correct_stat_user.csv, algo_correct_stat_rankgroup.csv
- yield_statistic_algo_each_enterprise: Problem statistics by company -> statistic_enterprise.csv


## Visualization
### 1. Trend for Month
This function allows you to identify the type of problem that is the latest trend and compare last month and this month<br><br>
**Client**
```
GET /trend
data : {
	"cur": "CURRENT MONTH",
	"bef": "BEFORE MONTH"
}

```
**Server**
```
response
[
	{
		"type":[
			"구현",
			"수학",
			"사칙연산",
			"그래프 이론",
			"자료 구조",
			"문자열"
			]
	},
	{
		"type":[
			"구현",
			"수학",
			"사칙연산",
			"그래프 이론",
			"그래프 탐색",
			"자료 구조"
			]
	}

]
```
**Result**<br>
![image](https://user-images.githubusercontent.com/83527620/174276239-9b30d070-c4b2-4623-a415-038371f05452.png)

### 2. Types of Problems What I have solved
This feature gives you a graph of which type of problem you solved the most<br><br>
**Client**
```
GET /analysis
data : {
	"name":"user id"
}

```

**Server**
```
Response
[
{
	"type": [
		"수학", 
		…(생략) 10 data
		],
	"try":[
		"120",
		…(생략) 10 data
	       ]
	"ans": [
		"0.35",
		…(생략) 10 data

},
{
  "type": [
		"구현", 
		…(생략) 10 data
		],
	"try":[
		“85672”,
		…(생략) 10 data
	       ]
	"ans": [
		“0.65”,
		…(생략) 10 data

},
{
	"user_ans": "39",
	"tier_ans": "52",
	"diff": "-13"
  "weak": [
			"정확도"
		],
	"strong":[
			"유형",
			"정답률"
		]
},

```
**Result**<br>
![image](https://user-images.githubusercontent.com/83527620/174278176-878c2d26-e880-4190-ab37-592057be7018.png)


### 3. Study Group with Other Users
You can join or build group
The service will recommend people on a level similar to yours<br><br>
**Client**
```
GET /study
data : {
	"name":"user id"
}

```

**Server**
```
response
{
	"weak": ["자료구조", "완전탐색"],
	"level": "골드 2",
  "user_name": [
			"0321minji",
			"054679860",
			"0913vision",
			"0h328",
			"oxe82de"
}
```
**Result**<br>
![image](https://user-images.githubusercontent.com/83527620/174278277-4a39415b-6e89-4df2-98b0-b69ac9f8065b.png)

### 4. Enterprise Acceptance Prediction Rate
Would you like to join a company? Use this feature to find out how acceptable your current situation is for the company<br><br>
**Client**
```
GET /company
data : {
	"name":"user id",
  "company":"company_id"
}

```

**Server**
```
response

{
	"percent":0.66,
	"weak":[
		"구현",
		"자료구조"
	]	
}

```
**Result**<br>
![image](https://user-images.githubusercontent.com/83527620/174278431-5a2dc333-8802-4a92-9bc3-8c8b7915e400.png)

## Demo
You can look at the functions on https://bigdata-server.herokuapp.com/

