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

var MAX_WAIT_DEFAULT = 5000;
var MIN_BYTES_DEFAULT = 2;


/**
 * main entry point for kafka client
 */
module.exports = function(options) {
  assert(options);
  var _options;
  var _cbt;
  var _brokers;
  var _topics;
  var _metaBroker;
  var _closing = false;
  var _polling = false;



  var _makeTopicKey = function(params) {
    return params.topic + '_' + params.partition;
  };



  var _makeBrokerKey = function(params) {
    return params.host + '_' + params.port;
  };



  var _determineTopicLeader = function(params, cb) {
    _metaBroker.metadata([params.topic], function(err, cluster) {
      if (err) { cb(err); return; }
      if (!cluster || !cluster.topics[params.topic].partitions[params.partition]) { cb(err); return; }

      var leaderId = cluster.topics[params.topic].partitions[params.partition].leaderId;
      var leader = cluster.brokers[leaderId];
      cb(null, leader);
    });
  };



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


/*
 * for V0.9 kafaka protocol
  var _offsetPosition = function(topic, cb) {
    if (topic.params.offset === undefined || topic.params.offset === -1) {
      topic.broker.offsetFetch({group: _options.group, topic: topic.params.topic, partition: topic.params.partition}, function(err, response) {
        if (response.topics[topic.params.topic].partitions[topic.params.partition].error === 0) {
          topic.params.offset = response.topics[topic.params.topic].partitions[topic.params.partition].offsetLo;
          cb(null, topic);
        }
        else {
          if (response.topics[topic.params.topic].partitions[topic.params.partition].offsetLo === 0xffffffff &&
              response.topics[topic.params.topic].partitions[topic.params.partition].offsetHi === 0xffffffff) {

            // BUG in Kafka - if the topic offset has not been set then an error condition is returned and
            // -1 is set for the offset, once an offset is set this condition is cleared
            topic.params.offset = 0;
            cb(null, topic);
          }
          else {
            cb(response.topics[topic.params.topic].partitions[topic.params.partition].error, null);
          }
        }
      });
    }
    else {
      cb(null, topic);
    }
  };
*/



  var _initiate = function(params, cb) {
    if (!_topics[_makeTopicKey(params)]) {
      _determineTopicLeader(params, function(err, leader) {
        if (err) { cb(err, null); return; }
        if (!_brokers[_makeBrokerKey(leader)]) {
          _brokers[_makeBrokerKey(leader)] = api({host: leader.host, port: leader.port, maxBytes: _options.maxBytes, clientId: _options.clientId});
          _brokers[_makeBrokerKey(leader)].tearUp(function(err) {
            if (err) { cb(err, null); return; }
            _brokers[_makeBrokerKey(leader)].connected = true;
            _topics[_makeTopicKey(params)] = { params: params, broker: _brokers[_makeBrokerKey(leader)] };
            _offsetPosition(_topics[_makeTopicKey(params)], function(err, topic) {
              cb(err, topic);
            });
          });
        }
        else {
          _topics[_makeTopicKey(params)] = { params: params, broker: _brokers[_makeBrokerKey(leader)] };
          _offsetPosition(_topics[_makeTopicKey(params)], function(err, topic) {
            cb(err, topic);
          });
        }
      });
    }
    else {
      cb(null, _topics[_makeTopicKey(params)]);
    }
  };



  var _emitStream = function(topic, emitter, index, response, cb) {
    if (index < response.messageSet.length) {
      emitter.emit('message', topic.params.offset + index + 1, response.messageSet[index], function() {
        _emitStream(topic, emitter, ++index, response, cb);
      });
    }
    else {
      cb(index);
    }
  };



  var _poll = function(topic, emitter, cb) {
    _polling = true;
    if (!emitter) {
      emitter = new events.EventEmitter();
      cb(null, emitter);
    }
    topic.broker.fetch(topic.params, function(err, response) {
      if (_closing) { return; }
      if (err) {
        emitter.emit('error', err);
      }
      else {
        _emitStream(topic, emitter, 0, response, function(commitCount) {
          if (commitCount > 0) {
            topic.params.offset = topic.params.offset + commitCount;
            emitter.emit('debug', 'setting offset to: ' + topic.params.offset);
            topic.broker.offsetCommit({group: _options.group, topic: topic.params.topic, partition: topic.params.partition, offset: topic.params.offset}, function(err, response) {
              if (err || response.hasErrors) {
                //commit errors disabled - reenable for 0.9
                //emitter.emit('error', err);
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
    _brokers = {};
    _topics = {};
    _.each(_options.brokers, function(broker) {
      _brokers[_makeBrokerKey(broker)] = api({host: broker.host, port: broker.port, maxBytes: _options.maxBytes, clientId: _options.clientId});
      if (!_metaBroker) {
        _metaBroker = _brokers[_makeBrokerKey(broker)];
      }
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
    assert(params.topic);
    assert(params.partition !== undefined);

    if (!params.maxWait) { params.maxWait = MAX_WAIT_DEFAULT; }
    if (!params.minBytes) { params.minBytes = MIN_BYTES_DEFAULT; }

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
    assert(params.topic);
    assert(params.partition !== undefined);

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
    assert(params.topic);

    _metaBroker.metadata([params.topic], function(err, cluster) {
      cb(err, cluster);
    });
  };



  /**
   * tearup first broker connection
   */
  var tearUp = function(cb) {
    _metaBroker.tearUp(function(err) {
      if (!err) { _metaBroker.connected = true; }
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
        _.each(_brokers, function(broker) {
          broker.tearDown();
        });
      }, MAX_WAIT_DEFAULT);
    }
    else {
      _.each(_brokers, function(broker) {
        broker.tearDown();
      });
    }
  };



  _construct();
  return {
    tearUp: tearUp,
    produce: produce,
    poll: poll,
    metadata: metadata,
    tearDown: tearDown
  };
};


