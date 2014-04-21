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

var BufferBuilder = require('buffer-builder');

exports.PRODUCE_API = 0;
exports.FETCH_API = 1;
exports.OFFSET_API = 2;
exports.METADATA_API = 3;
exports.OFFSETCOMMIT_API = 8;
exports.OFFSETFETCH_API = 9;



exports.appendStringAsBytes = function(buf, str) {
  if (!str || str.length === 0) {
    buf.appendUInt32BE(0xffffffff);
  }
  else {
    buf.appendUInt32BE(str.length);
    buf.appendString(str);
  }
};



exports.appendString = function(buf, str) {
  if (!str || str.length === 0) {
    buf.appendUInt16BE(0xffff);
  }
  else {
    buf.appendUInt16BE(str.length);
    buf.appendString(str);
  }
};



var correlation = function(correlationId) {
  this.buf.appendUInt32BE(correlationId);
  return this;
};



var client = function(clientName) {
  exports.appendString(this.buf, clientName);
  return this;
};



var group = function(group) {
  exports.appendString(this.buf, group);
  return this;
};



var timestamp = function() {
  this.buf.appendUInt32BE(0xffffffff);
  this.buf.appendUInt32BE(0xffffffff);
  return this;
};



var topic = function(topic) {
  this.buf.appendUInt32BE(0x00000001);
  exports.appendString(this.buf, topic);
  return this;
};



var partition = function(partitionNumber) {
  this.buf.appendUInt32BE(0x00000001);
  this.buf.appendUInt32BE(partitionNumber);
  return this;
};



var end = function() {
  return this.buf.get();
};



exports.encode = function(apiKey) {
  var buf = new BufferBuilder();
  buf.appendUInt16BE(apiKey);
  buf.appendUInt16BE(0x0000); // attributes
  return { buf: buf,
           correlation: correlation,
           client: client,
           group: group,
           timestamp: timestamp,
           topic: topic,
           partition: partition,
           end: end};
};

