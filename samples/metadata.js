'use strict';

var kafkaesque = require('../lib/kafkaesque')({brokers: [{host: 'localhost', port: 9092}],
                                               clientId: 'fish',
                                               group: 'cheese',
                                               maxBytes: 2000000});
kafkaesque.tearUp(function() {
  kafkaesque.metadata({topic: 'testing123'}, function(err, metadata) {
    console.log(JSON.stringify(metadata, null, 2));
    kafkaesque.tearDown();
  });
});

