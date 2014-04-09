'use strict';


var kafkaesque = require('../lib/kafkaesque')({brokers: [{host: 'localhost', port: 9092}],
                                               maxBytes: 2000000});
kafkaesque.tearUp(function() {
  //kafkaesque.poll({topic: 'testing123', partition: 2}, function(err, kafka) {
  kafkaesque.poll({topic: 'request', partition: 0}, function(err, kafka) {
    console.log(err);

    kafka.on('message', function(message, commit) {
      console.log(JSON.stringify(message));
      commit();
    });

    kafka.on('error', function(error) {
      console.log(JSON.stringify(error));
    });

  });
});

