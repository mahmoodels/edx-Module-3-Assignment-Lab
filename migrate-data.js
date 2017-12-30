const _ = require('lodash');
const async = require('async');
const MongoClient = require('mongodb').MongoClient;
const addr = require('./data/m3-customer-address-data.json');
const dat = require('./data/m3-customer-data.json');

let cnt = parseInt(process.argv[2], 10);
let url = 'mongodb://localhost:27017';

let tsks = [];
let task = (coll, ind, lenth, callback) => {
    console.log(`started processing data from ${ind} to ${ind + lenth}`)
    let customers = [];
    for (i = 0; i < lenth; i++) {
        let currInd=ind + i;
        let newObj = _.defaults(dat[currInd], addr[currInd]);//merge two docs
        customers.push(newObj);
    }
    coll.insert(customers, (errr, res) => {
        callback(errr, res);
    });
};

MongoClient.connect(url, (err, client) => {
    if (err) return console.error(err);
    let db = client.db('edx_Module_3_Assignment_db');
    let coll = db.collection('customers');

    for (j = 0; j < dat.length; j += cnt) {
        let indx = j;
        tsks.push((callback) => task(coll, indx, cnt, callback));
    }

    async.parallel(tsks, (erro, resul) => {
        if (erro) return console.error('Err M', erro);
        console.log('Done');
        client.close();
        process.exit(1);
    });
});