const express = require('express');
const app = express();
app.use(express.static(__dirname+ '/public'))
app.get('/', (req, res) => {
    res.sendFile(__dirname+'/public/home.html')
});

app.get('/trend', (req, res) => {
    console.log(req.query.cur);
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


app.listen(8000, () => {
    console.log('server is listening at localhost:8080');
});
