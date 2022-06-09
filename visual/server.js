const express = require('express');
const app = express();
app.use(express.static(__dirname+ '/public'))
app.get('/', (req, res) => {
    res.sendFile(__dirname+'/public/home.html')
});

app.listen(8000, () => {
    console.log('server is listening at localhost:8080');
});