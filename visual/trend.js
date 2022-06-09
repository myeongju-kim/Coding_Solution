function data(){
    var path = require("path");
    var fs = require("fs");
    var filePath = path.join(__dirname, "analysis_data/may.csv");
    var data=fs.readFileSync(filePath, {encoding:"utf8"})
    var rows=data.split("\n");
    var row=rows[0].split(",");
    console.log(row[1])
}