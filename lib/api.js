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
var net = require('net');
var events = require('events')
var hexy = require('hexy');

var envelope = require('./message/request/envelope');
var meta = require('./message/request/metadata');
var prod = require('./message/request/produce');
var fech = require('./message/request/fetch');
var off = require('./message/request/offset');
var groupCoord = require('./message/request/groupCoordinator');
var offCommit = require('./message/request/offsetCommit');
var offFetch = require('./message/request/offsetFetch');
var joinGrop = require('./message/request/joinGroup');
var syncGrop = require('./message/request/syncGroup');
var hbeat = require('./message/request/heartbeat');
var listGroup = require('./message/request/listGroups');
var describeGroup = require('./message/request/describeGroups');

var response = require('./message/response/common');
var metaResponse = require('./message/response/metadata');
var prodResponse = require('./message/response/produce');
var fetchResponse = require('./message/response/fetch');
var offsetResponse = require('./message/response/offset');
var groupCoordResponse = require('./message/response/groupCoordinator');
var offCommitResponse = require('./message/response/offsetCommit');
var offFetchResponse = require('./message/response/offsetFetch');
var joinGroupResponse = require('./message/response/joinGroup');
var syncGroupResponse = require('./message/response/syncGroup');
var heartbeatResponse = require('./message/response/heartbeat');
var listGroupResponse = require('./message/response/listGroups');
var describeGroupResponse = require('./message/response/describeGroups');
var kb = require('./kbuf');


/**
 * kafkaesque low level client
 */
module.exports = function(options) {
  assert(options);
  var _options;
  var _socket;
  var _cbt;
  var _rcvBuf;

  var _groupEvents;


  var sendMsg = function(msg, correlationId, cb) {
    /*console.log('snd -----------------------------------------------');
    console.log('');
    console.log(hexy.hexy(msg));
    console.log('');
    console.log('-----------------------------------------------');*/
    _socket.write(msg, function(err){
      if (err) {
        _cbt.remove(correlationId);
        cb(err);
      }
    });
  };



  /**
   * write a metadata request to kafka, storing the callback.
   *
   * topics: array of topics to retreive information on
   * cb: callback
   */
  var metadata = function(topics, cb) {
    var correlationId = _cbt.put(metaResponse(cb));
    var msg = envelope(meta.encode()
                           .correlation(correlationId)
                           .client(_options.clientId)
                           .topics(topics)
                           .end());
    sendMsg(msg, correlationId, cb);
  };



  /**
   * write a produce request to kafka
   *
   * params:
   *   topic: the topic to write to
   *   partition: the partition to write to
   *
   * messages: an array of messages to write to kafkia, messages may be
   *   - a string
   *   - an array of string
   *   - an array of objects of the form {key: ..., value: ...}
   *   if key value pairs exist the key will be written to kafka for reference purposes
   *   otherwise a null key will be used
   */
  var produce = function(params, messages, cb) {
    var correlationId = _cbt.put(prodResponse(cb));
    var msg = envelope(prod.encode()
                           .correlation(correlationId)
                           .client(_options.clientId)
                           .timeout()
                           .topic(params.topic)
                           .partition(params.partition)
                           .messages(messages)
                           .end());
    sendMsg(msg, correlationId, cb);
  };



  /**
   * write a fetch request to kafka
   *
   * params:
   *   topic: the topic to write to
   *   partition: the partition to write to
   *   offset: the offset to fetch from
   *   maxWait: the maximum wait time in ms
   *   minBytes: the minimum number of bytes that should be available before a response is sent
   */
  var fetch = function(params, cb) {
    // console.log(params)
    var correlationId = _cbt.put(fetchResponse(cb));
    var msg = envelope(fech.encode()
                           .correlation(correlationId)
                           .client(_options.clientId)
                           .maxWait(params.maxWait)
                           .minBytes(params.minBytes)
                           .topic(params.topic)
                           .partition(params.partition)
                           .offset(params.offset)
                           .maxBytes(_options.maxBytes)
                           .end());
    // console.log(hexy.hexy(msg))
    sendMsg(msg, correlationId, cb);
  };



  /**
   * request the latest offset from kafka
   *
   * i.e. the OLD offset api, not commit / fetch
   */
  var offset = function(params, cb) {
    var correlationId = _cbt.put(offsetResponse(cb));
    var msg = envelope(off.encode()
                          .correlation(correlationId)
                          .client(_options.clientId)
                          .replica()
                          .topic(params.topic)
                          .partition(params.partition)
                          .timestamp()
                          .maxOffsets()
                          .end());
    sendMsg(msg, correlationId, cb);
  };


  /**
   * write an offset request to kafka
   *
   * params:
   *   group: the consumer group id
   *   topic: the topic to commit on
   *   partition: the partition to commit on
   */
  var offsetFetch = function(params, cb) {
    // console.log(params)
    var correlationId = _cbt.put(offFetchResponse(cb));
    var msg = envelope(offFetch.encode(params.fetchFromCoordinator ? 1 : 0)
                               .correlation(correlationId)
                               .client(_options.clientId)
                               .group(params.group)
                               .topic(params.topic)
                               .partition(params.partition)
                               .end());
    sendMsg(msg, correlationId, cb);
  };


  /**
   * write a commit request to kafka
   *
   * params:
   *   group: the consumer group id
   *   topic: the topic to commit on
   *   partition: the partition to commit on
   *   offset: the offset to commit
   */
  var offsetCommit = function(params, cb) {
    var correlationId = _cbt.put(offCommitResponse(cb));
    var msg = envelope(offCommit.encode()
                                .correlation(correlationId)
                                .client(_options.clientId)
                                .group(params.group)
                                .topic(params.topic)
                                .partition(params.partition)
                                .offset(params.offset)
                                // .when(params.when) //disabled in api ver1
                                .meta(params.meta)
                                .end());

    sendMsg(msg, correlationId, cb);
  };


  var groupCoordinator = function (params, cb) {
    var correlationId = _cbt.put(groupCoordResponse(cb));
    var msg = envelope(groupCoord.encode()
                               .correlation(correlationId)
                               .client(_options.clientId)
                               .group(params.group)
                               .end());

    sendMsg(msg, correlationId, cb);
  };


  var joinGroup = function (params, cb) {
    if (!_groupEvents) {
      _groupEvents = new events.EventEmitter();
    }

    var correlationId = _cbt.put(joinGroupResponse(function (err, group) {
      if (err) {
        _groupEvents.emit('error', err);
        if (cb) { cb(err); }
        return;
      }
      cb(err, group, _groupEvents);

      _groupEvents.emit('syncRequired', group);
    }));
    var msg = envelope(joinGrop.encode()
                               .correlation(correlationId)
                               .client(_options.clientId)
                               .group(params.group)
                               .sessionTimeout(params.sessionTimeout)
                               .member(params.memberId)
                               .subscription(params.subscriptions)
                               .end());
    // console.log(hexy.hexy(msg))

    sendMsg(msg, correlationId, cb);
  };

  var syncGroup = function (params, cb) {
    var correlationId = _cbt.put(syncGroupResponse(function (err, state) {
      // console.log('newState', err, state);
      if (err) {
        _groupEvents.emit('error', err);
        if (cb) { cb(err); }
        return;
      }
      if (cb) { cb(err, state); }

      if (_groupEvents) {
        _groupEvents.emit('syncState', state);
      }
    }));
    var msg = envelope(syncGrop.encode()
                               .correlation(correlationId)
                               .client(_options.clientId)
                               .group(params.group)
                               .generation(params.generation)
                               .member(params.memberId)
                               .groupAssignment(params.groupAssignment)
                               .end());

    sendMsg(msg, correlationId, cb);
  };

  var heartbeat = function (params, cb) {
    var correlationId = _cbt.put(heartbeatResponse(function (err, res) {
      if (err && err !== 27){
        cb(err);
      }

      if(err === 27 && _groupEvents) {
        _groupEvents.emit('rejoinRequired');
      }
    }));

    var msg = envelope(hbeat.encode()
                               .correlation(correlationId)
                               .client(_options.clientId)
                               .group(params.group)
                               .generation(params.generation)
                               .member(params.memberId)
                               .end());
    // console.log(hexy.hexy(msg))

    sendMsg(msg, correlationId, cb);
  };

  var listGroups = function (params, cb) {
    var correlationId = _cbt.put(listGroupResponse(cb));
    var msg = envelope(listGroup.encode()
                               .correlation(correlationId)
                               .client(_options.clientId)
                               .end());

    sendMsg(msg, correlationId, cb);
  };

  var describeGroups = function (params, cb) {
    var correlationId = _cbt.put(describeGroupResponse(cb));
    var msg = envelope(describeGroup.encode()
                               .correlation(correlationId)
                               .client(_options.clientId)
                               .groups(params.groups)
                               .end());

    sendMsg(msg, correlationId, cb);
  };

  /**
   * construct the kafka client
   */
  var construct = function() {
    _cbt =  require('./cbt')();
    _options = options;
  };



  /**
   * tearup the connection to kafka
   */
  var tearUp = function(cb) {
    // return early if already connected
    if (_socket) {
      return cb(null, {host: _options.host, port: _options.port});
    }

    _socket = net.createConnection(_options.port, _options.host);

    _socket.on('connect', function() {
      if (cb) {
        cb(null, {host: _options.host, port: _options.port});
      }
    });

    _socket.on('data', function(buf) {
      var res;
      var rblock;
      var decoder;

      if (!_rcvBuf) {
        _rcvBuf = kb();
      }
      _rcvBuf.append(buf);
      res = response.decodeHead(_rcvBuf.get());
      // console.log(res)

      // buffer can have more than one msg...
      // don't want to drop msgs
      while (_rcvBuf.length() > 0 && _rcvBuf.length() >= res.size + 4) {
        decoder = _cbt.remove(res.correlation);
        // console.log('decoder', decoder)
        rblock = decoder.decode(_rcvBuf.get(), res);

        /*console.log('rcv -----------------------------------------------');
        console.log('');
        console.log(hexy.hexy(_rcvBuf.get()));
        console.log('');
        console.log('-----------------------------------------------');//*/
        _rcvBuf.slice(res.size + 4);

        if (_rcvBuf.length() > 0) {
          res = response.decodeHead(_rcvBuf.get());
        }

        decoder.callback(rblock.err, rblock.result);
      }
    });

    _socket.on('end', function() {
      // console.log('socket end');
    });

    _socket.on('timeout', function(){
      console.log('socket timeout');
    });

    _socket.on('drain', function(){
      // console.log('******** socket drain');
    });

    _socket.on('error', function(err){
      console.log('SOCKET ERROR: ' + err);
    });

    _socket.on('close', function(){
      // console.log('socket close');
    });
  };


  var tearDown = function() {
    _socket.end();
  };

  construct();

  return {
    tearUp: tearUp,
    tearDown: tearDown,
    metadata: metadata,
    produce: produce,
    fetch: fetch,
    offset: offset,
    groupCoordinator: groupCoordinator,
    offsetCommit: offsetCommit,
    offsetFetch: offsetFetch,
    joinGroup: joinGroup,
    syncGroup: syncGroup,
    heartbeat: heartbeat,
    listGroups: listGroups,
    describeGroups: describeGroups,
    host: options.host,
    port: options.port
  };
};
