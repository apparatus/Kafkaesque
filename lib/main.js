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
var BufferBuilder = require('buffer-builder');
var hexy = require('hexy');

var envelope = require('./message/request/envelope');
var meta = require('./message/request/metadata');
var prod = require('./message/request/produce');
var fech = require('./message/request/fetch');
var comm = require('./message/request/commit');
var offs = require('./message/request/offset');

var response = require('./message/response/common');
var metaResponse = require('./message/response/metadata');
var prodResponse = require('./message/response/produce');
var fetchResponse = require('./message/response/fetch');
var commitResponse = require('./message/response/commit');
var offsetResponse = require('./message/response/offset');



/**
 * main entry point for kafka client
 */
module.exports = function(options) {
  assert(options);
  var _options;
  var _socket;
  var _cbt;
  var _pollLock;
  var _rcvBuf;



  var sendMsg = function(msg, correlationId, cb) {
    if (_pollLock) {
      cb('Send rejected, poll in progress');
    }
    else {
      _pollLock = true;
      _socket.write(msg, function(err){
        if (err) {
          _pollLock = false;
          _cbt.fetch(correlationId);
          cb(err);
        }
      });
    }
  };



  /**
   * write a metadata request to kafka, storing the callback.
   *
   * topics: array of topics to retreive information on
   * cb: callback
   */
  var metadata = function(topics, cb) {
    var correlationId = _cbt.track(metaResponse(cb));
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
    var correlationId = _cbt.track(prodResponse(cb));
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
    var correlationId = _cbt.track(fetchResponse(cb));
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
  var commit = function(params, cb) {
    var correlationId = _cbt.track(commitResponse(cb));
    var msg = envelope(comm.encode()
                           .correlation(correlationId)
                           .client(_options.clientId)
                           .group(params.group)
                           .topic(params.topic)
                           .partition(params.partition)
                           .offset(params.offset)
                           .end());
    console.log(hexy.hexy(msg));
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
  var offset = function(params, cb) {
    var correlationId = _cbt.track(offsetResponse(cb));
    var msg = envelope(offs.encode()
                           .correlation(correlationId)
                           .client(_options.clientId)
                           .group(params.group)
                           .topic(params.topic)
                           .partition(params.partition)
                           .end());
    sendMsg(msg, correlationId, cb);
  };



  /**
   * construct the kafka client
   */
  var construct = function() {
    _cbt =  require('./cbt')();
    _options = options;
    _pollLock = false;
  };



  /**
   * tearup the connection to kafka
   */
  var tearUp = function(cb) {
    _socket = net.createConnection(_options.port, _options.host);

    _socket.on('connect', function() {
      if (cb) {
        cb();
      }
    });

    _socket.on('data', function(buf) {
      var res;
      var decoder;

      if (!_rcvBuf) {
        _rcvBuf = new BufferBuilder();
      }
      _rcvBuf.appendBuffer(buf);
      res = response.decodeHead(_rcvBuf.get());

      if (res.size + 4 === _rcvBuf.length) {
        console.log(hexy.hexy(_rcvBuf.get()));
        _pollLock = false;
        decoder = _cbt.lookup(res.correlation);
        decoder.decode(_rcvBuf.get(), res, decoder.callback);
        _rcvBuf = null;
      }
    });

    _socket.on('end', function() {
    });

    _socket.on('timeout', function(){
    });

    _socket.on('drain', function(){
    });

    _socket.on('error', function(err){
      console.log('ERROR: ' + err);
    });

    _socket.on('close', function(){
    });
  };



  var tearDown = function() {
  };



  construct();
  return {
    tearUp: tearUp,
    tearDown: tearDown,
    metadata: metadata,
    produce: produce,
    fetch: fetch,
    commit: commit,
    offset: offset
  };
};

