'use strict';

var kafkaesque = require('../lib/kafkaesque')({brokers: [{host: 'localhost', port: 9092}],
                                               clientId: 'fish',
                                               group: 'cheese',
                                               maxBytes: 2000000});
kafkaesque.tearUp(function() {
  kafkaesque.produce({topic: 'testing123', partition: 0}, ['wotcher mush', 'orwlight geezer'],
                     function(err, response) {
    console.log(response);
    kafkaesque.tearDown();
  });
});


