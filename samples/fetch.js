'use strict';

var kafkaesque = require('../lib/kafkaesque')({brokers: [{host: 'localhost', port: 9092}],
                                               clientId: 'fish',
                                               group: 'cheese',
                                               maxBytes: 1024*1024});

// this is the poll handler, passed to .poll()
// this is called per partition that kafkaesque connects to
// when invoking .poll()
var poll = function (err, kafka) {
  console.log(err);

  kafka.on('message', function(message, commit) {
    console.log(JSON.stringify(message));
    commit();
  });

  kafka.on('error', function(error) {
    console.log(JSON.stringify(error));
  });
};



// to fetch for all partitions (both valid)
kafkaesque.poll({topic: 'testing'}, poll)
kafkaesque.poll('testing', poll)

// to fetch for partition 0
kafkaesque.poll({topic: 'testing' partition: 0}, poll)
