'use strict';

var kafkaesque = require('../lib/kafkaesque')({brokers: [{host: 'localhost', port: 9092}],
                                               clientId: 'fish',
                                               group: 'cheese',
                                               maxBytes: 1024*1024});

// all valid
// specify the topic and partition to produce to,
// produce the array of strings
// callback optional
consumer.produce({topic: 'testing', partition: 0}, ['message form 1'], function(err, res) {})

// specify the topic to produce to,
// kafkaesque will choose the partition to produce to (round-robin style)
// produce the string
consumer.produce({topic: 'testing'}, 'message form 2')

// specify the topic to produce to,
// kafkaesque will choose the partition to produce to (round-robin style)
// produce the string
consumer.produce('testing', 'message form 3')
