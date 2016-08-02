# kafkaesque [![Build Status][travis-build-stat-svg]][travis-build-link]

[![NPM][npm-download-stat-svg]][npm-link]

## A Node.js Kafka client
kafkaesque is a node.js client for Apache Kafka. This client supports v0.8.1 and upwards of the Kafka protocol. Kafkaesque does not require any connection to zookeeper as it uses the kafka [wire protocol][wire-protocol] to determine how it should best connect and manage connections to the cluster. You need only provide Kafkaesque with the details of a single broker in any Kafka cluster and it will figure out the rest.

Kafkaesque gives you fine grained control over what topics and partitions you connect to and it gives a very easy to use interface to let this module auto-assign partitions for you. This module should be useful for Kafka beginners and veterans alike!

## Prerequisites
You will need to install Apache Kafka 0.8.1 or greater.

## Installation

```
npm install kafkaesque
```

## Quickstart

Produce Example:

```javascript
// create a kafkaesque client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: 'localhost', port: 9092}]
});
kafkaesque.produce('testing', 'message 1');
```

Simple Consumer Example:
In the following we create a 'Simple Consumer' (in kafka terminology). This is a consumer which consumes from specified partitions, not auto assigned partitions.

```javascript
// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: 'localhost', port: 9092}]
});
kafkaesque.poll({topic: 'testing', partition: 0}, poll);

function poll (err, partition) {
  // handle each message
  partition.on('message', function(message, commit) {
    console.log(JSON.stringify(message));
    // once a message has been successfull handled, call commit to advance this
    // consumers position in the topic / parition
    commit();
  });

  // report errors
  partition.on('error', function(error) {
    console.log(JSON.stringify(error));
  });
}
```

Auto Balanced Group Member Example:
In the following we create an Auto Balanced Group Member. This is a consumer which subscribes to topics, joins a group and then gets auto-assigned partitions from its topic subscriptions. This allows for easy horizontal scaling of kafkaesque consumers.

```javascript
// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: 'localhost', port: 9092}]
});

// subscribe to a partitioned topic
// this topic can have a large number of partitions, but using kafkaesque,
// these can be split evenly between members of the group.
kafkaesque.subscribe('a-partitioned-topic');

// connect gives a nice EventEmitter interface for receiving messages
kafkaesque.connect(function (err, kafka) {
  if (err) {
     throw new Error('problem connecting to auto managed kafka cluster' + err);
  }

  // handle each message
  kafka.on('message', function(message, commit) {
    console.log(message);
    // once a message has been successfull handled, call commit to advance this
    // consumers position in the topic / parition
    commit();
  });

  // report errors
  kafka.on('error', function(error) {
    console.log(error);
  });
});
```

## API

### Configuration:
```javascript
var kafkaesque = require('kafkaesque')(configObj);
```
* `configObj` - An object that can have the following properties assigned for configuration customisation
	* `brokers` - An array of one (or more) kafka brokers in the format `{host: … , port: …}` (default: `[{host: 'localhost', port: 9092}]`)
	* `clientId` - The name for this client (default: `'kafkaesque' + Math.floor(Math.random() * 100000000)`)
	* `group` - The group that this client is a member of. Used extensively for auto-managing partition assignments. (default: `kafkaesqueGroup`)
	* `maxBytes` - The maximum number of bytes to return in any one message (default: `1024*1024`)
	* `minBytes` - The minimum number of bytes to return in any one message (default: `1`)
	* `sessionTimeout` - The amount of milliseconds until this consumer will timeout when member of a consumer group which auto handles assignments, etc. (default: `6000`)
	* `heartbeat` - The amount of milliseconds between heartbeats while a member of a group. (default: `2500`)
  * `maxWait` - The default amount of time that the kafka broker should wait to send a reply to a fetch request if the fetch reply is smaller than the minBytes. (default: `5000`)

### `kafkaesque` methods

* `kafkaesque.produce(params, message(s), cb)` - send messages to kafka
	* `params` - Either a `String` or an `Object`.
		* If a `String` - this is the topic to publish to. Kafkaesque internally chooses the partition in this case. * If an `Object`- This have the following properties:
			* `params.topic` - The topic to publish to. REQUIRED IF PARAMS = OBJECT.
			* `params.partition` - The partition to publish to. If undefined then Kafkaesque internally chooses the partition.
	* `message(s)` - Either a `String`, an `Array` of `String`s, or an `Array` of `Object`s.
		* If a `String` - This is the message to publish.
		* If an `Array` of `String`s - These are the strings to publish.
		* If an `Array` of `Object`s - They must have the following properties:
			* `Key` - The key of the message. _REQUIRED_
			* `Value` - The value of the message. _REQUIRED_
			* Useful if using the key-value feature of kafka commits.
		```
		REMINDER: An OBJECT is not valid input. JSON.stringify() it first.
		```
	* `cb` - The function to callback when message(s) is(/are) published. _OPTIONAL_.

* `kafkaesque.poll(params, cb)` - Poll kafka for messages, in the kafka `simple consumer` style.
	* `params` - Either a `String` or an `Object`.
		* If a `String` - The name of the topic you wish to poll. This will poll ALL partitions for that topic, and the callback `cb` will be invoked PER partition connection.
		* If an `Object` - This can have the following properties:
			* `topic` - The topic to poll for messages. _REQUIRED_.
			* `partition`- The partition you want to read messages from in a topic. If this is undefined then kafkaesque will read from all partitions. This means the callback(`cb`) will be invoked PER partition. _OPTIONAL_.
			* `offset` - The offset you want to read messages from in the commit log. _OPTIONAL_.
			* `maxWait` - The default amount of time that the kafka broker should wait to send a reply to a fetch request if the fetch reply is smaller than the minBytes. _OPTIONAL_.
			* `minBytes` - The minimum number of bytes to return in any one message. _OPTIONAL_.
	* `cb(err, partition)` - The callback function to be invoked for each partition `poll` connects to. Take the following parameters: `(err, partition)`.
		* `err` - Any error which happened while connecting to the cluster.
		* `partition` - An `EventEmitter` Object. Events listed below:
			* `message` - Emitted with two parameters, the `message` string and the `commit` callback. `commit` must be called to consume the next message from the partition. Refer to Quickstart `Simple Consumer` for example.
			* `error` - Emitted on error.
			* `debug` - General debug info.

* `kafkaesque.connect(cb)` - Let Kafkaesque manage your partition assignments for you, based on topic subscriptions. Connect to the cluster and get an easy to manage event emmiter in your callback.
	* `cb` - Function. Takes the following form: `function(err, kafka)`.
		* `err` - Error object if there is a problem connecting to cluster.
		* `kafka` - an `EventEmitter` object for kafka messages. Emits the following events:
			* `message` - Emitted with two parameters, the `message` string and the `commit` callback. `commit` must be called to consume the next message. Refer to Quickstart `Auto Managed Consumer` for example.
			* `error` - Emitted on error.
			* `connect` - Emitted when first connected to the cluster.
			* `debug` - General debug info.
			* `rebalance.start` - Emitted when the kafka group needs to rebalance/reassign the subscribed topic partitions to members.
			* `rebalance.end` - Emitted after rebalancing.
			* `electedLeader` - Emitted when elected leader, and your client must assign partitions. Don't worry though, kafkaesque manages this for you.

* `kafkaesque.disconnect(cb)` - Shut down internal connections to the kafka cluster gracefully and let the group coordinator know you're leaving so the group can resync sooner and no longer need to wait for your client to timeout.

* `kafkaesque.subscribe(topics)` - subscribe to auto-managed topics.
	* `topics` - A `String` or an `Array` of `String`s to subscribe to in an auto-managed group. Each String must be a topic name. This will cause the client to consume from the topics passed in here in an auto-assigned group if `connect` was called.

* `kafkaesque.unsubscribe(topics)` - unsubscribe to auto-managed topics.
	* `topics` - A `String` or an `Array` of `String`s to unsubscribe to in an auto-managed group. Each String must be a topic name. This will cause the client to no longer consume from the topics passed in here in an auto-assigned group if `connect` was called.

* `kafkaesque.metadata(topic, cb)` - return metatdata on a topic.
	* `topic` - the topic to return metadata on.

* `kafkaesque.listGroups(cb)` - list all groups.
	* `cb` - Callback function. Arguments: `function(err, res)`
		* `err` - any error encountered when trying to list all groups.
		* `res` - The results.

* `kafkaesque.describeGroups(groups, cb)` - describe each group here.
	*	`groups` - A `String` or an `Array` of `String`s representing group names.
	* `cb` - Callback function. Arguments: `function(err, res)`
		* `err` - any error encountered when trying to describe the groups.
		* `res` - The results.

* `kafkaesque.tearDown()` - tear down the connection to the kafka cluster _DEPRECATED_. Please use `disconnect()` instead.

* `kafkaesque.tearUp(cb)` - tear up connection to the kafka cluster. _DEPRECATED_. Should no longer be neccessary.

## Samples
Provided under the samples folder. All of the samples assume a kafka installation on localhost and unless stated otherwise they require that you have created a topic 'testing' on your cluster.


The following will return metadata information on the topic 'testing'.
```bash
cd samples
node metadata.js
```

The following will post messages to the 'testing' topic.
```bash
node produce.js
```

The following will fetch messages in the 'testing' topic, after the latest commited offset for members of this group.
```bash
node fetch.js
```

The following will fetch messages from the beginning for the 'testing' topic.
```bash
node fetchFromStart.js
```

The following will join an auto-managed group, and subscribe to the 'testing' topic in that group.
```bash
node auto-assigned-member.js
```

## Contributing

This module encourages open participation. If you feel you can help in any way, or discover any Issues, feel free to [create an issue][issue] or [create a pull request][pr]!

If you wish to read more on our guidelines, feel free to checkout the concise [contribution file][contrib]

## Support

This project is kindly sponsored by [nearForm](http://www.nearform.com).

We hope that this code is useful, please feel free to get in touch if you need help or support: @pelger or @thekemkid.

## License
Copyright Peter Elger and other contributors 2013 - 2016, Licensed under [MIT][].

[travis-build-link]: https://travis-ci.org/apparatus/Kafkaesque
[travis-build-stat-svg]: https://travis-ci.org/apparatus/Kafkaesque.svg?branch=group-membership
[npm-link]: https://nodei.co/npm/kafkaesque/
[npm-download-stat-svg]: https://nodei.co/npm/kafkaesque.png?downloads=true&downloadRank=true&stars=true
[wire-protocol]: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
[issue]: https://github.com/apparatus/kafkaesque/issues
[pr]: https://github.com/apparatus/kafkaesque/pulls
[MIT]: ./LICENSE
[contrib]: ./CONTRIBUTING.md
