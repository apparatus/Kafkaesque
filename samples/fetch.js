'use strict';


var kafkaesque = require('../lib/kafkaesque')({brokers: [{host: 'localhost', port: 9092}],
                                               clientId: 'fish',
                                               maxBytes: 2000000});
kafkaesque.tearUp(function() {
  kafkaesque.poll({topic: 'testing', partition: 0}, function(err, kafka) {

    kafka.on('message', function(message, commit) {
      console.log(JSON.stringify(message));
      commit();
    });

    kafka.on('error', function(error) {
      console.log(JSON.stringify(error));
    });

  });
});

