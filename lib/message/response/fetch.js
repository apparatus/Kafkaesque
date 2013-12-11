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



var decodeMessageset = function(buf, offset, result) {
  var startOffset = offset;
  var messageSet = [];
  var message = {};
  var s;

  try {
    while (result.messageSetSize > offset - startOffset) {
      message = {};

      message.offsetHi = buf.readUInt32BE(offset);
      offset += 4;
      message.offsetLo = buf.readUInt32BE(offset);
      offset += 4;

      message.size = buf.readUInt32BE(offset);
      offset += 4;

      message.crc = buf.readUInt32BE(offset);
      offset += 4;

      message.magic = buf.readUInt8(offset);
      offset += 1;

      message.attributes = buf.readUInt8(offset);
      offset += 1;

      s = common.readBytesAsString(buf, offset);
      message.key = s.str;
      offset = s.offset;

      s = common.readBytesAsString(buf, offset);
      message.value = s.str;
      offset = s.offset;

      messageSet.push(message);
    }
  }
  catch(exp) {

    // ignore range errors: kafka uses a maxBytes parameter to indicate the
    // maximum bytes that a response should include. Unfortunately kafka hard
    // cuts a message rather than cutting at a logical message boundary. Until
    // this is fixed...
    if (exp.message !== 'Trying to access beyond buffer length') {
      throw(exp);
    }
  }
  result.messageSet = messageSet;
  return result;
};



var decode = function(buf, result, cb) {
  var offset = 8;
  var s;

  result.topicCount = buf.readUInt32BE(offset);
  offset += 4;
  s = common.readString(buf, offset);
  result.topic = s.str;
  offset = s.offset;

  result.partitionCount = buf.readUInt32BE(offset);
  offset += 4;
  result.partitionId = buf.readUInt32BE(offset);
  offset += 4;

  result.errorCode = buf.readUInt16BE(offset);
  offset += 2;

  result.highWatermarkOffsetHi = buf.readUInt32BE(offset);
  offset += 4;
  result.highWatermarkOffsetLo = buf.readUInt32BE(offset);
  offset += 4;

  result.messageSetSize = buf.readUInt32BE(offset);
  offset += 4;

  result = decodeMessageset(buf, offset, result);

  return {err: result.errorCode === 0 ? null : result.errorCode, result: result};
};



module.exports = function(callback) {
  return {
    decode: decode,
    callback: callback
  };
};

