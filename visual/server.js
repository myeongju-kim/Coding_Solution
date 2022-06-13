const express = require('express');
const app = express();
app.use(express.static(__dirname+ '/public'))
app.get('/', (req, res) => {
    res.sendFile(__dirname+'/public/home.html')
});

app.get('/trend', (req, res) => {
    //달 입력 받음
    const cur_mon=req.query.cur;
    const bef_mon=req.query.bef;
    const fs = require("fs");
    const path=require("path");
    const csvPath=path.join(__dirname,'analysis_data','may.csv')
    const csv=fs.readFileSync(csvPath,"utf-8")
    const rows=csv.split("\n")
    const top1=rows[0].split(',')
    const top2=rows[1].split(',')
    const top3=rows[2].split(',')
    const top4=rows[3].split(',')
    const top5=rows[4].split(',')
    const top6=rows[5].split(',')
  
    const csvPath2=path.join(__dirname,'analysis_data','april.csv')
    const csv2=fs.readFileSync(csvPath2,"utf-8")
    const rows2=csv2.split("\n")
    const top12=rows2[0].split(',')
    const top22=rows2[1].split(',')
    const top32=rows2[2].split(',')
    const top42=rows2[3].split(',')
    const top52=rows2[4].split(',')
    const top62=rows2[5].split(',')


    const agt=[
        {
            "type":top1[1],
            "try":top1[2]
        },
        {
            "type":top2[1],
            "try":top2[2]
        },
        {
            "type":top3[1],
            "try":top3[2]
        },
        {
            "type":top4[1],
            "try":top4[2]
        },
        {
            "type":top5[1],
            "try":top5[2]
        },
        {
            "type":top6[1],
            "try":top6[2]
        }, 
        {
            "type":top12[1],
            "try":top12[2]
        },
        {
            "type":top22[1],
            "try":top22[2]
        },
        {
            "type":top32[1],
            "try":top32[2]
        },  
        {
            "type":top42[1],
            "try":top42[2]
        },
        {
            "type":top52[1],
            "try":top52[2]
        },
        {
            "type":top62[1],
            "try":top62[2]
        },
    ]
    res.send(agt);
});

app.get('/analysis', (req, res) => {
    //사용자 정보 요청
    // algo_correct_stat_user.csv: [user, user_rank, type, totalsubmit, correct, wrong, correct_rate]
    // algo_correct_stat_rankgroup.csv: [user_rank, type, totalsubmit, correct, wrong, correct_rate]입니다
    const fs = require("fs");
    const path=require("path");
    const csvPath=path.join(__dirname,'analysis_data','algo_correct_stat_user.csv')
    const csv=fs.readFileSync(csvPath,"utf-8")
    const rows=csv.split("\n");
    const user_rows=[]
    var usum=0;
    var total=0;
    for(var i=0; i<rows.length; i++){
        var temp=rows[i].split(",")
        if(temp[0]=="mjoo1106"){
            user_rows.push(temp)
            total+=Number(temp[3])
            usum+=Number(temp[4])
        }
    }

   user_rows.sort(function(a,b){
        return Number(b[3])-Number(a[3])
    });
    const apt=[]
    for(var i=0; i<10; i++){
        apt.push({"type":user_rows[i][2],
                "try":user_rows[i][3],
                "ans":Number(user_rows[i][6].slice(0,4))})
    }
    apt.push(Math.round((usum/total)*100))

    const fs2 = require("fs");
    const path2=require("path");
    const csvPath2=path2.join(__dirname,'analysis_data','algo_correct_stat_rankgroup.csv')
    const csv2=fs2.readFileSync(csvPath2,"utf-8")
    const rows2=csv2.split("\n");
    const user_rows2=[]
    var usum2=0;
    var total2=0;
    for(var i=0; i<rows2.length; i++){
        var temp2=rows2[i].split(",")
        if(temp2[0]=="골드 3"){
            user_rows2.push(temp2)
            total2+=Number(temp2[2])
            usum2+=Number(temp2[3])
        }
    }
    apt.push(Math.round((usum2/total2)*100))

    user_rows.sort(function(a,b){
        return Number(b[6])-Number(a[6])
    });
    for(var i=0; i<10; i++){
        apt.push({"type":user_rows[i][2],
                "ans":Number(user_rows[i][6].slice(0,4))})
    }
    user_rows2.sort(function(a,b){
        return Number(b[2])-Number(a[2])
    });
    for(var i=0; i<10; i++){
        apt.push({"type":user_rows2[i][1],
                "try":user_rows2[i][2],
                "ans":Number(user_rows2[i][5].slice(0,4))})
    }


    res.send(apt);
});
app.get('/company_analysis', (req, res) => {
    // user|line_score1|line_score2|line_inferior|line_superior|line_acceptance_probability|
    // kakao_score1|kakao_score2|kakao_inferior|kakao_superior|kakao_acceptance_probability|
    // coupang_score1|coupang_score2|coupang_inferior|coupang_superior|coupang_acceptance_probability|
    // samsung_score1|samsung_score2|samsung_inferior|samsung_superior|samsung_acceptance_probability|
    // naver_score1|naver_score2|naver_inferior|naver_superior|naver_acceptance_probability
    const fs = require("fs");
    const path=require("path");
    const csvPath=path.join(__dirname,'analysis_data','user_with_acceptance_prediction.csv')
    const csv=fs.readFileSync(csvPath,"utf-8")
    const rows=csv.split("\n");
    for(var i=0; i<rows.length; i++){
        var temp=rows[i].split(",")
        if(temp[0]=="mjoo1106"){
            break;
        }
    }
    const arg=[];
    for(i=5; i<=25; i+=5){
        if(temp[i].indexOf("E")!=-1){
            arg.push(0);
        }
        else{
            arg.push(Math.round(Number(temp[i])*100))
        }
    }
    res.send(arg);
});
app.get('/study', (req, res) => {
    // user|line_score1|line_score2|line_inferior|line_superior|line_acceptance_probability|
    // kakao_score1|kakao_score2|kakao_inferior|kakao_superior|kakao_acceptance_probability|
    // coupang_score1|coupang_score2|coupang_inferior|coupang_superior|coupang_acceptance_probability|
    // samsung_score1|samsung_score2|samsung_inferior|samsung_superior|samsung_acceptance_probability|
    // naver_score1|naver_score2|naver_inferior|naver_superior|naver_acceptance_probability
    const fs = require("fs");
    const path=require("path");
    const csvPath=path.join(__dirname,'analysis_data','algo_correct_stat_user.csv')
    const csv=fs.readFileSync(csvPath,"utf-8")
    const rows=csv.split("\n");
    var arg=[];
    var sign=0;
    for(var i=0; i<rows.length; i++){
        var temp=rows[i].split(",")
        if(temp[1]=="골드 2"){
             if(arg.indexOf(temp[0])==-1)
                arg.push(temp[0]);        
        }
        if(arg.length>=5)
            break;
    }
  
    res.send(arg);
});
app.listen(8000, () => {
    console.log('server is listening at localhost:8080');
});