'use strict';

var kafkaesque = require('../lib/kafkaesque')({brokers: [{host: 'localhost', port: 9092}],
                                               clientId: 'fish',
                                               group: 'cheese',
                                               maxBytes: 1024*1024});

kafkaesque.metadata({topic: 'testing'}, function(err, metadata) {
  console.log(JSON.stringify(metadata, null, 2));
  kafkaesque.tearDown();
});
