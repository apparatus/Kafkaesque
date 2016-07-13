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

var common = require('./common');
var BufferBuilder = require('buffer-builder');
var crc32 = require('buffer-crc32');


var sessionTimeout = function (timeInMs) {
  this.buf.appendUInt32BE(timeInMs);

  return this;
}


var member = function (memberId) {
  common.appendString(this.buf, memberId);

  return this;
}


var subscription = function (topics) {
  //protocoltype
  common.appendString(this.buf, 'consumer');

  //length of the group protocols array
  this.buf.appendUInt32BE(0x00000001)

  common.appendString(this.buf, 'range');

  // metadata
  var bytes = new BufferBuilder();
  bytes.appendUInt16BE(0x0000); // version

  bytes.appendUInt32BE(topics.length);
  topics.forEach(function (topic) {
    common.appendString(bytes, topic);
  });

  // userdata bytes
  bytes.appendUInt32BE(0);
  // we could actually put some json in the userdata bytes section using this...

  this.buf.appendUInt32BE(bytes.length)
  this.buf.appendBuffer(bytes);

  return this;
}


exports.encode = function() {
  var ret = common.encode(common.JOINGROUP_API);

  ret.sessionTimeout = sessionTimeout;
  ret.member = member;
  ret.subscription = subscription;

  return ret;
};
