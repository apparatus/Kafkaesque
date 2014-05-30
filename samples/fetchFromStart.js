'use strict';

var kafkaesque = require('../lib/kafkaesque')({brokers: [{host: 'localhost', port: 9092}],
                                               clientId: 'fish',
                                               group: 'cheese',
                                               maxBytes: 2000000});
kafkaesque.tearUp(function() {
  kafkaesque.poll({topic: 'testing123', partition: 0, offset: 0}, function(err, kafka) {
    console.log(err);

    kafka.on('message', function(offset, message, commit) {
      console.log(JSON.stringify(message));
      commit();
    });

    kafka.on('error', function(error) {
      console.log(JSON.stringify(error));
    });

  });
});


