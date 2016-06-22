/*
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

'use strict';

var assert = require('assert');
var api = require('./api');
var events = require('events');
var _ = require('underscore');
var reInterval = require('reInterval');


// _options = {
//   brokers: [{host: ..., port: ... }, ...],
//   group: ...,
//   maxBytes: ...,
//   clientId: ...
// }

/**
 * main entry point for kafka client
 */
module.exports = function(options) {
  assert(options);
  var _options;
  var _cbt;
  var _brokers;
  var _topics;
  var _heartbeatInterval;

  // handy broker refs
  var _groupCoordinator;
  var _metaBroker;

  var _kafkaEvents;
  var _groupMemberId;

  var _assignments = [];
  var _subscriptions = [];
  var _producePartition = 0;
  var _closing = false;
  var _polling = false;
  var _resyncing = false;



  var _noop = function () {};


  var _makeTopicKey = function(params) {
    if (params.partition === undefined) {
      return params.topic;
    }
    return params.topic + '_' + params.partition;
  };



  var _makeBrokerKey = function(params) {
    return params.host + '_' + params.port;
  };


  var _getTopicMetadata = function(params, cb) {
    var metadata = function () {
      _metaBroker.metadata([params.topic], function(err, cluster) {
        if (err) { cb(err); return; }
        if (!cluster || !_.findWhere(cluster.topics, {topicName: params.topic})) { cb(err); return; }
        cb(null, cluster);
      });
    };

    if (!_metaBroker.connected) {
      _metaBroker.tearUp(function (err) {
        if (err) { return cb(err); }
        _metaBroker.connected = true;
        metadata();
      });
    }
    else {
      metadata();
    }
  };


  var _determinePartitionLeader = function(params, cb) {
    // console.log('determinePartLeader', params)
    _getTopicMetadata(params, function (err, cluster) {
      if (err) { cb(err); return; }
      if (!cluster || !_.findWhere(_.findWhere(cluster.topics, {topicName: params.topic}).partitions, {partitionId: params.partition})) { cb(err); return; }

      var leaderId = _.findWhere(_.findWhere(cluster.topics, {topicName: params.topic}).partitions, {partitionId: params.partition}).leaderId;
      var leader = _.findWhere(cluster.brokers, {brokerId: leaderId});
      // console.log('leader', leader)

      cb(null, leader);
    });
  };



  function _nextValidPartition(params) {
    if(params.partition !== undefined) {
      return params.partition;
    }
    if(!_topics[_makeTopicKey(params)]) {
      return _producePartition;
    }
    return ((_producePartition++) % _topics[_makeTopicKey(params)].topics[params.topic].partitions.length);
  }

/*
 * for V0.8.1.x kafaka protocol
  var _offsetPosition = function(topic, cb) {
    if (topic.params.offset === undefined || topic.params.offset === -1) {
      topic.broker.offset({group: _options.group, topic: topic.params.topic, partition: topic.params.partition}, function(err, response) {
        if (response.topics[topic.params.topic].partitions[topic.params.partition].error === 0) {
//          topic.params.offset = response.topics[topic.params.topic].partitions[topic.params.partition].offsetLo;
          topic.params.offset = response.topics[topic.params.topic].partitions[topic.params.partition].offsets[0].offsetLo;
          cb(null, topic);
        }
        else {
          cb(response.topics[topic.params.topic].partitions[topic.params.partition].error, null);
        }
      });
    }
    else {
      cb(null, topic);
    }
  };
 */

/*
 * for V0.8.2 (i.e. 0.9) kafaka protocol */
  var _offsetPosition = function(topic, cb) {
    if (topic.params.offset === undefined || topic.params.offset === -1) {
      var fetchFrom = topic.broker;
      if (topic.params.fetchFromCoordinator) {
        fetchFrom = _groupCoordinator;
      }

      fetchFrom.offsetFetch({
        group: _options.group,
        topic: topic.params.topic,
        partition: topic.params.partition,
        fetchFromCoordinator: topic.params.fetchFromCoordinator
      }, function(err, response) {
        // console.log('error in offFetch', err)
        if (err) { return cb(err); }
        // console.log('getting offset', err, _.findWhere(response.topics, {name: topic.params.topic}).partitions)
        // console.log('getting offset', err, response)
        var partition = _.findWhere(_.findWhere(response.topics, {name: topic.params.topic}).partitions, {partitionId: topic.params.partition});
        if (partition.error === 0) {
          topic.params.offset = partition.offsetLo;
          cb(null, topic);
        }
        else {
          if (partition.offsetLo === 0xffffffff &&
              partition.offsetHi === 0xffffffff) {

            // BUG in Kafka - if the topic offset has not been set then an error condition is returned and
            // -1 is set for the offset, once an offset is set this condition is cleared
            topic.params.offset = 0;
            cb(null, topic);
          }
          else {
            cb(partition.error, null);
          }
        }
      });
    }
    else {
      cb(null, topic);
    }
  };


  var _connectToPartition = function (params, leader, cb) {
    var topicKey = _makeTopicKey(params);
    var leaderKey = _makeBrokerKey(leader);
    var cacheTopic = function() {
      _topics[topicKey] = {
        params: params,
        broker: _brokers[leaderKey]
      };

      _offsetPosition(_topics[topicKey], cb);
    };

    var connect = function(err) {
      if (err) {
        return cb(err, null);
      } else {
        _brokers[leaderKey].connected = true;
        cacheTopic();
      }
    };

    if (!_brokers[leaderKey]) {
      _brokers[leaderKey] = api({host: leader.host, port: leader.port, maxBytes: _options.maxBytes, clientId: _options.clientId});
      _brokers[leaderKey].tearUp(connect);
    }
    else {
      if (!_brokers[leaderKey].connected) {
        _brokers[leaderKey].tearUp(connect);
      } else {
        cacheTopic();
      }
    }
  };


  var _initiate = function(params, cb) {
    // console.log('init params', params)
    if (!_topics[_makeTopicKey(params)]) {
      if (params.partition !== undefined) {
        _determinePartitionLeader(params, function (err, leader) {
          if (err) {
            return cb(err, null);
          }
          // console.log('leader details', params, leader)
          _connectToPartition(params, leader, cb);
        });
      } else {
        _getTopicMetadata(params, function(err, cluster) {
          if (err) {
            return cb(err, null);
          }

          // set _topics[_makeTopicKey(params)] = true to cause enclosing
          // if (!_topics[_makeTopicKey(params)]) return false
          // which will make the else run (GOOD)
          _topics[_makeTopicKey(params)] = cluster;

          _.findWhere(cluster.topics, {topicName: params.topic}).partitions.forEach(function (partition){
            var leader = _.findWhere(cluster.brokers, {brokerId: partition.leaderId});

            // params needs to be cloned, else the partitionId is overwrote because
            // of pass-by-ref
            var p = JSON.parse(JSON.stringify(params));
            p.partition = partition.partitionId;

            _connectToPartition(p, leader, cb);
          });
        });
      }
    }
    else {
      if (params.partition !== undefined) {
        cb(null, _topics[_makeTopicKey(params)]);
      } else {
        Object.keys(_topics).forEach(function (topicKey) {
          // if topicKey contains the topic
          if (topicKey.indexOf(params.topic + '_') > -1) {
            cb(null, _topics[topicKey]);
          }
        });
      }
    }
  };



  var _emitStream = function(topic, emitter, index, response, cb) {
    if (index < response.messageSet.length) {
      // console.log('*emit msg', response.messageSet[index])
      // console.log('*parsed', JSON.parse(response.messageSet[index].value))
      emitter.emit('message', response.messageSet[index], function() {
        // process.nextTick is needed for very large messageSets, so we don't get
        // a stack overflow
        if(++index % 88 === 0){
          process.nextTick(function () { _emitStream(topic, emitter, index, response, cb); });
        } else {
          _emitStream(topic, emitter, index, response, cb);
        }
      });
    }
    else {
      cb(index);
    }
  };



  var _poll = function(topic, emitter, cb) {
    _polling = true;
    if (_closing) { return; }

    if (!emitter) {
      emitter = new events.EventEmitter();
      cb(null, emitter);
    }
    topic.broker.fetch(topic.params, function(err, response) {
      if (err) {
        emitter.emit('error', err);
      }
      else {
        _emitStream(topic, emitter, 0, response, function(commitCount) {
          if (commitCount > 0) {
            topic.params.offset = topic.params.offset + commitCount;
            emitter.emit('debug', 'setting offset to: ' + topic.params.offset);
            topic.broker.offsetCommit({
              group: _options.group,
              topic: topic.params.topic,
              partition: topic.params.partition,
              offset: topic.params.offset,
              when: new Date().getTime(),
              meta: 'kafkaesque'
            }, function(err, response) {
              if (err || response.hasErrors) {
                // enabled for 0.8.2 (i.e. 0.9)
                emitter.emit('error', err);
              }
              emitter.emit('debug', 'reentering poll');
              _poll(topic, emitter, cb);
            });
          }
          else {
            emitter.emit('debug', 'no data - reentering poll');
            _poll(topic, emitter, cb);
          }
        });
      }
    });
  };


  /**
   * construct the kafka clients
   */
  var _construct = function() {
    _cbt =  require('./cbt')();
    _options = options;

    // apply defaults
    // the min bytes of an incoming reply
    _options.minBytes = _options.minBytes || 1;

    // the max bytes of an incoming reply
    _options.maxBytes = _options.maxBytes || 1024 * 1024;

    // the group the client will join when using `.connect()`
    _options.group = _options.group || 'kafkaesqueGroup';

    // the clientID when connecting to kafka
    _options.clientId = _options.clientId || 'kafkaesque' + Math.floor(Math.random() * 100000000);

    // the array of brokers
    _options.brokers = _options.brokers || [{host: 'localhost', port: 9093}];

    // the amount of time it should take for this clients session within the group
    // to timeout
    _options.sessionTimeout = _options.sessionTimeout || 6000;

    // the amount of time to take to emit a heartbeat msg
    _options.heartbeat = _options.heartbeat || 2500;

    // the default amount of time that the kafka broker shoudl wait to send
    // a reply to a fetch request if the fetch reply is smaller than the minBytes
    _options.maxWait = _options.maxWait || 5000;

    _brokers = {};
    _topics = {};
    _groupMemberId = '';
    _.each(_options.brokers, function(broker) {
      broker = {
        host: broker.host,
        port: broker.port,
        maxBytes: _options.maxBytes,
        minBytes: _options.minBytes,
        clientId: _options.clientId
      };

      _brokers[_makeBrokerKey(broker)] = api(broker);
    });

    _metaBroker = _brokers[_makeBrokerKey(_options.brokers[0])];
  };


  var _joinGroup = function (cb) {
    cb = cb || _noop;
    _groupCoordinator.joinGroup({
      group: _options.group,
      subscriptions: _subscriptions,
      sessionTimeout: _options.sessionTimeout,
      memberId: _groupMemberId
    }, cb);
  };


  var  _connectToGroupCoordinator = function (cb) {
    // _metaBroker.metadata(['my-replicated-partitioned-topic'], console.log);

    _metaBroker.groupCoordinator({group: _options.group}, function (err, result) {
      if (err) {
        err = new Error('kafkaesque encountered a problem connecting to group coordinator: ' + err);
        cb(err);
      }
      // console.log(result)
      if (result.errorCode === 0 && result.coordinatorHost === '') {
        // should we make _this_ broker the groupCoordinator?
        // sounds good to me!
        _groupCoordinator = _metaBroker;
        cb(err);
      } else {
        var apiOpts = {
          host: result.coordinatorHost,
          port: result.coordinatorPort,
          maxBytes: _options.maxBytes,
          minBytes: _options.minBytes,
          clientId: _options.clientId
        };
        var brokerKey = _makeBrokerKey(apiOpts);
        _brokers[brokerKey] = api(apiOpts);
        _groupCoordinator = _brokers[brokerKey];
        _groupCoordinator.tearUp(function (err) {
          if (!err) {
            _groupCoordinator.connected = true;
          }
          cb(err);
        });
      }
    });
  };


  // connect this kafkaesque instance to the meta broker
  var _connectToMetaBroker = function (cb) {
    _metaBroker.tearUp(function (err) {
      if (!err) {
        _metaBroker.connected = true;
      }
      cb(err);
    });
  };


  /**
   * long poll kafka for data, will repeatedly poll on a topic / partition until
   * endPoll is called. On calling poll an event emitter for the topic partition pair
   * is returned, this should be used to receive events on the given channel
   *
   * params:
   *   topic        - the topic name, required
   *   partition    - the partition id, required
   *   offset       - the starting offset, if unspecified kafkaesque uses the latest commmited position
   *   maxWait      - the maximum poll wait time, if unspecified defaults to 5 seconds
   *   minBytes     - the minimum bytes that should be available before returning, if unspecified defaults to 50 bytes
   *
   * cb: callback of the form function(err, eventEmitter, commit):
   *   err          - error condtion
   *   eventEmitter - event emitter for this topic / partition
   *   commit       - commit function to be called on message processing to advance the kafka commit log
   *                  events MUST be comitted in order to release the next message to the event emitter
   */
  var poll = function(params, cb) {
    if (_.isString(params)) {
      params = { topic: params };
    }

    assert(params.topic);
    assert(cb);
    _closing = false;

    if (!params.maxWait) { params.maxWait = _options.maxWait; }
    if (!params.minBytes) { params.minBytes = _options.minBytes; }

    _initiate(params, function(err, topic) {
      if (err) { cb(err, null); return; }
      _poll(topic, null, cb);
    });
  };



  /**
   * send data to a kafka topic / partition
   *
   * params:
   *   topic        - the topic name, required
   *   partition    - the partition id, required
   *
   * messages:
   *   an array of messages to write to kafkia, messages may be
   *     - a string
   *     - an array of string
   *     - an array of objects of the form {key: ..., value: ...}
   *   if key value pairs exist the key will be written to kafka for reference purposes
   *   otherwise a null key will be used
   */
  var produce = function(params, messages, cb) {
    cb = cb || _noop;
    if (_.isString(params)) {
      params = { topic: params };
    }
    assert(params.topic);
    // assert(params.partition !== undefined);
    if (params.partition === undefined) {
      params.partition = _nextValidPartition(params);
    }

    _initiate(params, function(err, topic) {
      if (err) { cb(err, null); return; }
      topic.broker.produce(topic.params, messages, function(err, response) {
        cb(err, response);
      });
    });
  };



  /**
   * make a metadata request to the kafka cluster
   *
   * params:
   *   topic        - the topic name, required
   */
  var metadata = function(params, cb) {
    cb = cb || _noop;
    assert(params.topic);

    _metaBroker.metadata([params.topic], function(err, cluster) {
      cb(err, cluster);
    });
  };

  var _assignPartitions = function (topics, members, params) {
    _metaBroker.metadata(topics, function(err, cluster) {
      // iterate through all topics, select that topics partitions from
      // the metadata, filter all members who are subbed to that topic,
      // then for each member, assign them partitions
      topics.forEach(function (topic) {
        var o = 0; //used for counting partition assignments
        var partitions = _.findWhere(cluster.topics, {topicName: topic}).partitions.sort(function (a, b) {
          if (a.partitionId < b.partitionId) {
            return -1;
          } else  if (a.partitionId > b.partitionId) {
            return 1;
          }
          return 0;
        });

        members
        .filter(function (member) {
          return _.contains(member.metadata.subscriptions, topic);
        })
        .forEach(function (member, i, arr) {
          //  math.floor (partitions.length / arr.length) = num partitions EVERY member should be subbed to
          // ((i+1) <= partitions.length%arr.length ? 1 : 0) = ensure remainder is applied too
          var numElems = Math.floor(partitions.length / arr.length) + ((i+1) <= partitions.length%arr.length ? 1 : 0);
          var assignment = {
            topic: topic,
            partitions: _.pluck(partitions.slice(o, o + numElems), 'partitionId')
          };
          o += numElems;
          _.findWhere(params.groupAssignment, {id: member.memberId}).memberAssignment.assignments.push(assignment);
        });
      });

      _groupCoordinator.syncGroup(params, _noop);
    });
  };

  var _assignGroup = function (params, group) {
    // leader must do things
    var topics = [];
    var members = [];

    // create an easy access array for members and store all members
    // partitions
    group.members.forEach(function (member) {
      members.push(member);
      topics = _.union(topics, member.metadata.subscriptions);
    });

    // sort members by id
    members = members.sort(function(a, b){
      if (a.memberId < b.memberId) {
        return -1;
      } else  if (a.memberId > b.memberId) {
        return 1;
      }
      return 0;
    });

    // pre populate the groupAssignments in the params with empty
    // assignments
    members.forEach(function (member) {
      params.groupAssignment.push({id: member.memberId, memberAssignment: {version: member.metadata.version, assignments: []}});
    });


    // if the topics length > 0, ensure we have the most recent metadata
    // for them
    if (topics.length > 0) {
      _assignPartitions(topics, members, params, group);
    } else {
      _groupCoordinator.syncGroup(params, _noop);
    }
  };

  var _syncGroup = function (group) {
    // commit & stop consuming
    _kafkaEvents.emit('rebalance.start');

    _resyncing = true;
    _heartbeatInterval.reschedule(_options.heartbeat);
    _groupMemberId = group.memberId;

    var params = {group: _options.group, memberId: _groupMemberId, generation: group.generationId, groupAssignment: []};

    // if leader
    if(_groupMemberId === group.leaderId) {
      _kafkaEvents.emit('electedLeader');
      _assignGroup(params, group);
    } else {
      _groupCoordinator.syncGroup(params, _noop);
    }
  };

  var _emitAssignmentStream = function(index, messageSet, cb) {
    if (_resyncing || _closing) { //we should jump out here, and not emit anything
      return cb(index);
    }

    if (index < messageSet.length) {
      _kafkaEvents.emit('message', messageSet[index], function() {
        // process.nextTick is needed for very large messageSets, so we don't get
        // a stack overflow
        if(++index % 88 === 0){
          process.nextTick(function () { _emitAssignmentStream(index, messageSet, cb); });
        } else {
          _emitAssignmentStream(index, messageSet, cb);
        }
      });
    }
    else {
      cb(index);
    }
  };

  var _pollAssignments = function (err, assignment) {
    if (err) {
      // console.log('here', err, assignment);
      return _kafkaEvents.emit('error', new Error('problem polling assigned partition and topic:' + err));
    }
    if (_closing || _resyncing) { return; }

    assignment.broker.fetch(assignment.params, function(err, response) {
      // console.log('problem spot', err, response)
      if (err) {
        _pollAssignments(err, assignment);
      } else {
        _emitAssignmentStream(0, response.messageSet, function(commitCount) {
          if (commitCount > 0) {
            //causing an invalid offset to be set?
            assignment.params.offset = assignment.params.offset + commitCount;

            _groupCoordinator.offsetCommit({
              group: _options.group,
              topic: assignment.params.topic,
              partition: assignment.params.partition,
              offset: assignment.params.offset,
              when: new Date().getTime(),
              meta: 'kafkaesque'
            }, function(err) {
              // console.log('asdf', err, response)
              _kafkaEvents.emit('debug', 'reentering poll');

              _pollAssignments(err, assignment);
            });
          } else {
            _kafkaEvents.emit('debug', 'no data - reentering poll');
            _pollAssignments(null, assignment);
          }
        });
      }
    });
  };

  var _initiateAssignments = function (cb) {
    _assignments.forEach(function (assignment) {
      assignment.partitions.forEach(function (partition) {
        _initiate({topic: assignment.topic, partition: partition, maxWait: _options.maxWait, fetchFromCoordinator: true}, cb);
      });
    });
    _resyncing = false;
    _kafkaEvents.emit('rebalance.end');
  };

  var _receivedAssignments = function (newState) {
    // fetch, start consuming
    _resyncing = false;
    _assignments = newState.topics;
    _initiateAssignments(_pollAssignments);
  };

  var _rejoinGroup = function () {
    _heartbeatInterval.reschedule(_options.heartbeat);

    _joinGroup(function (err) {
      if (err) {
        console.log('something is wrong:', err);
      }
    });
  };

  var connect = function (cb) {
    assert(cb);

    _closing = false;
    if (!_kafkaEvents) {
      _kafkaEvents = new events.EventEmitter();
    }

    _connectToMetaBroker(function (err) {
      if (err) {
        return cb(err);
      }

      _connectToGroupCoordinator(function (err) {
        if (err) {
          return cb(err);
        }
        _joinGroup(function (err, group, groupEmitter) {
          if (err) {
            return cb(err);
          }
          _groupMemberId = group.memberId;

          cb(null, _kafkaEvents);

          _kafkaEvents.emit('connect');

          groupEmitter.on('syncRequired', _syncGroup);

          groupEmitter.on('syncState', _receivedAssignments);

          groupEmitter.on('rejoinRequired', _rejoinGroup);

          groupEmitter.on('error', function (errorCode) {
            console.log('error code', errorCode);
          });

          _heartbeatInterval = reInterval(function heartbeat() {
            if (!_resyncing) {
              _groupCoordinator.heartbeat({group: _options.group, generation: group.generationId, memberId: _groupMemberId}, _noop);
            }
          }, _options.heartbeat);
        });
      });
    });
  };

  /**
   * tearup first broker connection
   * DEPRECATED
   */
  var tearUp = function(cb) {
    // after we tear up, maybe we should make some requests in the same event loop
    // iteration?
    _metaBroker.tearUp(function(err) {
      if (err) { cb(err); }
      _metaBroker.connected = true;

      cb(err);
    });
  };



  /**
   * end all polls and teardown all connections to kafka
   */
  var tearDown = function() {
    if (_polling) {
      _closing = true;
      setTimeout(function() {
        if( _closing) {
          _.each(_brokers, function(broker) {
            broker.tearDown();
          });
        }
      }, _options.maxWait);
    }
    else {
      _.each(_brokers, function(broker) {
        broker.tearDown();
      });
    }
  };

  var disconnect = function () {
    tearDown();

    if (_polling) {
      _closing = true;
      setTimeout(function() {
        if (_closing) {
          _metaBroker.tearDown();
          _groupCoordinator.tearDown();
          _metaBroker.connected = false;
          _groupCoordinator.connected = false;
        }
      }, _options.maxWait);
    }
    else {
      _metaBroker.tearDown();
      _groupCoordinator.tearDown();
      _metaBroker.connected = false;
      _groupCoordinator.connected = false;
    }
  };

  var subscribe = function (topics) {
    if (!_.isArray(topics)) {
      topics = [topics];
    }

    _subscriptions = _.union(_subscriptions, topics);

    if(_groupCoordinator && _groupCoordinator.connected) {
      _joinGroup();
    }
  };

  var unsubscribe = function (topics) {
    if (!_.isArray(topics)) {
      topics = [topics];
    }

    _subscriptions = _.difference(_subscriptions, topics);

    if(_groupCoordinator && _groupCoordinator.connected) {
      _joinGroup();
    }
  };

  // initialise/_construct the kafkaesque object
  _construct();

  return {
    connect: connect,
    tearUp: tearUp,
    produce: produce,
    poll: poll,
    subscribe: subscribe,
    unsubscribe: unsubscribe,
    metadata: metadata,
    disconnect: disconnect,
    tearDown: tearDown
  };
};
