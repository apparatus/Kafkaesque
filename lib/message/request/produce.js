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

var _ = require('underscore');
var common = require('./common');
var BufferBuilder = require('buffer-builder');
var crc32 = require('buffer-crc32');

var REQUIRED_ACKS = 1;
var TIMEOUT_MS = 2000;



/**
 * calculate message bytes size:
 *
 * 8 bytes offset + 4 bytes size + 4 bytes CRC + 1 byte Magic + 1 bytes attributes + 
 * 4 bytes key length + 4 bytes value length + key lenght + value length
 */
var messageByteSize = function(msg) {
  return 4 + 1 + 1 + 4 + 4 + msg.key.length + Buffer.byteLength(msg.value);
};



/**
 * calculate size of the entire message block:
 * include 8 bytes offset + 4 byte size for each message
 */
var messageSetByteSize = function(msgs) {
  var size = 0;
  _.each(msgs, function(msg) {
    size += 8 + 4;
    size += messageByteSize(msg);
  });
  return size;
};



var messages = function(msgsIn) {
  var buf = this.buf;
  var msgs = [];

  if (!_.isArray(msgsIn)) {
    msgsIn = [msgsIn];
  }

  _.each(msgsIn, function(msg) {
    var obj = {};
    obj.key = msg.key ? msg.key : '';
    obj.value = msg.value ? msg.value : msg;
    msgs.push(obj);
  });

  buf.appendUInt32BE(messageSetByteSize(msgs));

  _.each(msgs, function(msg) {
    var msgBuf = new BufferBuilder();
    var rawMsg;

    buf.appendUInt32BE(0x00000000);  // offset first 4 bytes
    buf.appendUInt32BE(0x00000000);  // offset last 4 bytes
    buf.appendUInt32BE(messageByteSize(msg));

    msgBuf.appendUInt8(0x02);  // magic number 
    msgBuf.appendUInt8(0x00);  // attributes
    common.appendStringAsBytes(msgBuf, msg.key);   // message key if present, otherwise -1
    common.appendStringAsBytes(msgBuf, msg.value); // message value if present, otherwise -1

    rawMsg = msgBuf.get();

    buf.appendUInt32BE(crc32.unsigned(msgBuf.get()));  // crc
    buf.appendBuffer(rawMsg);
  });
  return this;
};



var timeout = function(timeout) {
  var to = timeout ? timeout : TIMEOUT_MS;
  this.buf.appendUInt16BE(REQUIRED_ACKS);
  this.buf.appendUInt32BE(to);
  return this;
};



/**
 * Encode a kafka produce request - assuming a single topic and partition
 */
exports.encode = function() {
  var ret = common.encode(common.PRODUCE_API);
  ret.timeout = timeout;
  ret.messages = messages;
  return ret;
};

