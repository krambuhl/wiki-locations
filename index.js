var queue = require('queue');
var request = require('request');
var cheerio = require('cheerio');
var mongo = require('mongodb').MongoClient
var _ = require('lodash');

var q = queue();
q.concurrency = 10;
q.timeout = 6000;

var count = 0;
var max = 200000; // only look through this many records

q.on('timeout', function(next, job) {
  console.log('job timed out, added back to queue');
  q.push(job);
  next();
});

var uniques = [];
var rootUrl = 'http://en.wikipedia.org';

function tunnel(url, meta) {
  if (q.length > max) return;

  q.push(function(cb) {
    if (uniques.indexOf(url) === -1) {
      uniques.push(url);
      request(rootUrl + url, function(error, response, html) {
        if(!error) {
          var $ = cheerio.load(html);

          var path = response.req.path;
          var title = $('title').text().split(' - Wikipedia')[0];
          var location = $('.infobox').find('.geo-dms').eq(0);
          var lat = location.find('.latitude').text();
          var long = location.find('.longitude').text();

          var links = $('#mw-content-text')
            .find('.reflist')
            .prev()
            .prevAll()
            .find('a')
            .filter('[href^="/wiki"]')
            .not('[href^="/wiki/File:"]')

          links.each(function() { 
            tunnel($(this).attr('href'), meta);
          });

          if (location.length > 0) {
            meta.insert([{
              title: title,
              path: path,
              url: rootUrl + path,
              lat: lat,
              long: long
            }], function() {
              console.log(['writing ', ++count, '/', q.length, ': ', title].join(''));
              cb();
            })
          } else {
            cb();
          }
        } else {
          cb();
        }
      });
    } else {
      cb();
    }    
  })
}

mongo.connect('mongodb://localhost:27017/wikimaps', function(err, db) {
  var meta = db.collection('meta');
  meta.find({}).toArray(function(err, docs) {
    // uniques = _.pluck(docs, 'path');
    tunnel('/wiki/Ladd\'s_Addition', meta);
    q.start(function() {
      console.log('done.')
    });
  })
});
