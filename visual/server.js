const express = require('express');
const WebHDFS=require("webhdfs");
var request=require("request");
const app = express();
app.use(express.static(__dirname+ '/public'))
app.get('/', (req, res) => {
    res.sendFile(__dirname+'/public/home.html')
});

app.get('/trend', (req, res) => {
    const url="your hdfs host name here";
    const port=50070;
    const cur_mon=req.query.cur;
    const bef_mon=req.query.bef;
    const dir_path="/user/root/static/monthly_count/"+cur_mon;
    const path="/webhdfs/v1/" + dir_path + "?op=LISTSTATUS&user.name=hdfs";
    const csv=url+':'+port+path;
    const rows=csv.split("\n")
    const top1=rows[0].split(',')
    const top2=rows[1].split(',')
    const top3=rows[2].split(',')
    const top4=rows[3].split(',')
    const top5=rows[4].split(',')
    const top6=rows[5].split(',')
  
    const dir_path2="/user/root/static/monthly_count/"+bef_mon;
    const path2="/webhdfs/v1/" + dir_path2 + "?op=LISTSTATUS&user.name=hdfs";
    const csv2=url+':'+port+path2;
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
    const baekjoon_id=req.query.name;
    const url="your hdfs host name here";
    const port=50070;
    const dir_path="/user/root/static/stat_correct/algo_correct_stat_user"
    const path="/webhdfs/v1/" + dir_path + "?op=LISTSTATUS&user.name=hdfs";
    const csv=url+':'+port+path;
    const rows=csv.split("\n");
    const user_rows=[]
    var usum=0;
    var total=0;
    var tier;
    for(var i=0; i<rows.length; i++){
        var temp=rows[i].split(",")
        if(temp[0]==baekjoon_id){
            user_rows.push(temp)
            tier=temp[1]
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


    const dir_path2="/user/root/static/stat_correct/algo_correct_stat_rankgroup"
    const path2="/webhdfs/v1/" + dir_path2 + "?op=LISTSTATUS&user.name=hdfs";
    const csv2=url+':'+port+path2;
    const rows2=csv2.split("\n");
    const user_rows2=[]
    var usum2=0;
    var total2=0;
    for(var i=0; i<rows2.length; i++){
        var temp2=rows2[i].split(",")
        if(temp2[0]==tier){
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
    const baekjoon_id=req.query.name;
    const url="your hdfs host name here";
    const port=50070;
    const dir_path="/user/root/static/stat_enterprise/user_with_acceptance_prediction"
    const path="/webhdfs/v1/" + dir_path + "?op=LISTSTATUS&user.name=hdfs";
    const csv=url+':'+port+path;
    const rows=csv.split("\n");
    for(var i=0; i<rows.length; i++){
        var temp=rows[i].split(",")
        if(temp[0]==baekjoon_id){
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
    const baekjoon_id=req.query.name;
    const url="your hdfs host name here";
    const port=50070;
    const dir_path="/user/root/static/stat_correct/algo_correct_stat_user"
    const path="/webhdfs/v1/" + dir_path + "?op=LISTSTATUS&user.name=hdfs";
    const csv=url+':'+port+path;
    const row=csv.split("\n");
    var tier;
    const u_row=[];
    for(var i=0; i<row.length; i++){
        var temp=ro[i].split(",")
        if(temp[0]==baekjoon_id){
            u_row.push(temp)
            tier=temp[1]
        }
    }
    u_row.sort(function(a,b){
        return Number(b[3])-Number(a[3])
    });

    var arg=[];
    for(var i=0; i<rows.length; i++){
        var temp=rows[i].split(",")
        if(temp[1]==tier){
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