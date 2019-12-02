'use strict';

const { Parser } = require('flora-sql-parser');
const express = require('express');
var bodyParser = require('body-parser')

// Constants
const PORT = 7001;
const HOST = '0.0.0.0';

// App
const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
  extended: true
}));
app.post('/ast', (req, res) => {
  const parser = new Parser();
  const sql = req.body.sql;
  const ast = parser.parse(sql);
  console.log("====================");
  console.log(JSON.stringify(ast));
  console.log("====================");
  res.send(JSON.stringify(ast));
});

app.listen(PORT, HOST);
console.log(`Running on http://${HOST}:${PORT}`);