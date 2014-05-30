# kafkaesque

## A Node.js Kafka client
kafkaesque is a node.js client for Apache Kafka supporting upwards of v0.8 of the Kafka protocol only. Kafkaesque does not require any connection to zookeeper, rather it uses the kafka metadata protocol request to determine how it should best connect to the cluster. You need only provide Kafkaesque with the details of a single broker in any Kafka cluster and it will figure out the rest.

The current 0.8 release of Kafka does not appear to support the full protocol set as described here: [https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol). Specifically the offset/commit/fetch API.

Kafkaesque will uses API as opposed to reading meta commit information from zookeeper when it is full supported in Kafka.

## Note
Kafkaesque has an implementation for the offset fetch/commit API, this is not funcitonal in the .8.x Kafka releases. Expected in .9.x release. 

## Prerequisites
You will need to install Apache Kafka 0.8.x or greater.

## Installation

```
npm install kafkaesque
```

## Quickstart

Produce example:

```
// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: 'localhost', port: 9092}],
  clientId: 'MrFlibble',
  maxBytes: 2000000
});

// tearup the client
kafkaesque.tearUp(function() {
  // send two messages to the testing topic
  kafkaesque.produce({topic: 'testing', partition: 0}, 
                     ['wotcher mush', 'orwlight geezer'], 
                     function(err, response) {
    // shutdown connection
    console.log(response);
    kafkaesque.tearDown();
  });
});
```

Consume example:

```
// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: 'localhost', port: 9092}],
  clientId: 'fish',
  maxBytes: 2000000
});

// tearup the client
kafkaesque.tearUp(function() {
  // poll the testing topic, kafakesque will determine the lead broker for this
  // partition / topic pairing and will emit messages as they become available
  // kafakesque will maintain the read position on the topic based on calls to 
  // commit()
  kafkaesque.poll({topic: 'testing', partition: 0}, 
                  function(err, kafka) {
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
  });
});
```

## Samples
Provided under the samples folder. All of the samples assume a kafka installation on localhost and require that you have created a topic 'testing' on your cluster.

````
cd samples
node metadata.js
````

Will return metadata information on the topic testing

````
node produce.js
````

Will post two messages to the testing topic

````
node fetch.js
````

Will consume messages from the testing topic. Note that the consume stores its position in the kafka commit log using the commit/offset/fetch API.


## Reference

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
	* messages - an array of string or object to send as messages

* poll(params, cb) - LONG poll kafka for messages
	* params.topic - the topic name, required
    * params.partition - the partition id, required
    * offset - the starting offset, if unspecified kafkaesque uses the latest commmited position against this topic / partition pair
    *  maxWait - the maximum poll wait time, if unspecified defaults to 5 seconds
    *   minBytes - the minimum bytes that should be available before returning, if unspecified defaults to 50 bytes

## Support
Hope that this code is useful, please feel free to get in touch if you need help or support: @pelger

