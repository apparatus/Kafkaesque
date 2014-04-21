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



var decode = function(buf) {
  var offset = 8;
  var off = {};
  var err = null;
  var s;

  var topicCount = buf.readUInt32BE(offset);
  offset = offset + 4;
  off.topics = {};
  for (var topicIdx = 0; topicIdx < topicCount; ++topicIdx) {
    var topic = { partitions: {}};

    s = common.readString(buf, offset);
    topic.name = s.str;
    offset = s.offset;

    var partitionCount = buf.readUInt32BE(offset);
    offset = offset + 4;
    for (var partitionIdx = 0; partitionIdx < partitionCount; ++partitionIdx) {
      var partition = {offsets: []};

      partition.partitionId = buf.readUInt32BE(offset);
      offset = offset + 4;

      partition.error = buf.readUInt16BE(offset);
      if (partition.error !== 0) {
        off.hasErrors = true;
        err = true;
      }
      offset = offset + 2;

      var offsetArrayLength = buf.readUInt32BE(offset);
      offset = offset + 4;

      for (var offsetIdx = 0; offsetIdx < offsetArrayLength; ++offsetIdx) {
        var offsetHi = buf.readUInt32BE(offset);
        offset = offset + 4;
        var offsetLo = buf.readUInt32BE(offset);
        offset = offset + 4;
        partition.offsets.push({offsetLo: offsetLo, offsetHi: offsetHi});
      }
      topic.partitions[partition.partitionId] = partition;
    }
    off.topics[topic.name] = topic;
  }
  return {err: err, result: off};
};



module.exports = function(callback) {
  return {
    decode: decode,
    callback: callback
  };
};

