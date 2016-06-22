# kafkaesque [![Build Status](https://travis-ci.org/thekemkid/Kafkaesque.svg?branch=group-membership)](https://travis-ci.org/thekemkid/Kafkaesque)

[![NPM](https://nodei.co/npm/kafkaesque.png?downloads=true&downloadRank=true&stars=true)](https://nodei.co/npm/kafkaesque/)

## A Node.js Kafka client
kafkaesque is a node.js client for Apache Kafka. This client supports v0.8.1 and upwards of the Kafka protocol. Kafkaesque does not require any connection to zookeeper as it uses the kafka [wire protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol) to determine how it should best connect and manage connections to the cluster. You need only provide Kafkaesque with the details of a single broker in any Kafka cluster and it will figure out the rest.

## Prerequisites
You will need to install Apache Kafka 0.8.1 or greater.

## Installation

```
npm install kafkaesque
```

## Quickstart

Produce example:

```javascript
// create a kafkaesque client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: 'localhost', port: 9092}]
});
kafkaesque.produce('testing', 'message 1');
```

Simple Consumer example:
In the following we create a 'simple consumer' (in kafka terminology). this is a consumer which consumes from specified partitions, not auto assigned partitions

```javascript
// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: 'localhost', port: 9092}]
});
kafkaesque.poll({topic: 'testing', partition: 0} poll);

function poll (err, kafka) {
  // handle each message
  kafka.on('message', function(message, commit) {
    console.log(JSON.stringify(message));
    // once a message has been successfull handled, call commit to advance this
    // consumers position in the topic / parition
    commit();
  });

  // report errors
  kafka.on('error', function(error) {
    console.log(JSON.stringify(error));
  });
}
```

## Samples
Provided under the samples folder. All of the samples assume a kafka installation on localhost and unless stated otherwise they require that you have created a topic 'testing' on your cluster.


The following will return metadata information on the topic 'testing'.
```bash
cd samples
node metadata.js
```



The following will post two messages to the 'testing' topic.
```bash
node produce.js
```

The following will fetch messages from the beginning for the 0 partition in the 'testing' topic.
```bash
node simple-fetch-from-beginning.js
```


## API

* Configuration
	* brokers - array of one or more kafka brokers in the format { host: … , Port: …}
	* clientId - reference name for this client
	* maxBytes - the maximum number of bytes to return in any one message

* tearUp(cb) - tear up connection to the kafka cluster

* tearDown() - tear down the connection to the kafka cluster

* metadata(params, cb) - return metatdata on a topic
	* params.topic - the topic name to return metadata on


* produce(params, messages, cb) - send messages to kafka
	* params.topic - the topic name to send to
	* params.partition - the partition to send to
	* messages - a string or an array of strings to send
        `NOTE: objects are not valid input. JSON.stringify() them first`

* poll(params, cb) - LONG poll kafka for messages
	* params.topic - the topic name, required
    * params.partition - the partition id, required
    * offset - the starting offset, if unspecified kafkaesque uses the latest commmited position against this topic / partition pair
    *  maxWait - the maximum poll wait time, if unspecified defaults to 5 seconds
    *   minBytes - the minimum bytes that should be available before returning, if unspecified defaults to 50 bytes

## Contributing

This module encourages open participation. If you feel you can help in any way, or discover any Issues, feel free to [create an issue][issue] or [create a pull request][pr]!

If you wish to read more on our guidelines, feel free to checkout the concise [contribution file][contrib]

## Support

This project is kindly sponsored by [nearForm](http://www.nearform.com).

We hope that this code is useful, please feel free to get in touch if you need help or support: @pelger or @thekemkid.

## License
Copyright Pelger and other contributors 2013, Licensed under [MIT][].

[issue]: https://github.com/pelger/kafkaesque/issues
[pr]:https://github.com/peleger/kafkaesque/pulls
[MIT]: ./LICENSE
[contrib]: ./CONTRIBUTING.md
