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



var brokerDecode = function(buf, offset, ret) {
  var broker = {};
  var s;
  var idx;

  broker.brokerId = buf.readUInt32BE(offset);
  offset = offset + 4;

  s = common.readString(buf, offset);
  broker.host = s.str;
  offset = s.offset;

  broker.port = buf.readUInt32BE(offset);
  offset = offset + 4;

  var topicCount = buf.readUInt32BE(offset);
  offset = offset + 4;
  broker.topics = [];
  for (var topicIdx = 0; topicIdx < topicCount; ++topicIdx) {
    var topic = {};

    topic.topicErr = buf.readUInt16BE(offset);
    offset = offset + 2;

    s = common.readString(buf, offset);
    topic.topicName = s.str;
    offset = s.offset;

    var partitionCount = buf.readUInt32BE(offset);
    offset = offset + 4;
    topic.partitions = [];
    for (var partitionIdx = 0; partitionIdx < partitionCount; ++partitionIdx) {
      var partition = {};

      partition.partitionErr = buf.readUInt16BE(offset);
      offset = offset + 2;

      partition.partitionId = buf.readUInt32BE(offset);
      offset = offset + 4;

      partition.leader = buf.readUInt32BE(offset);
      offset = offset + 4;

      var replicaCount = buf.readUInt32BE(offset);
      offset = offset + 4;
      partition.replicas = [];
      for (idx = 0; idx < replicaCount; ++ idx) {
        partition.replicas.push(buf.readUInt32BE(offset));
        offset = offset + 4;
      }

      var isrCount = buf.readUInt32BE(offset);
      offset = offset + 4;
      partition.isr = [];
      for (idx = 0; idx < isrCount; ++idx) {
        partition.isr.push(buf.readUInt32BE(offset));
        offset = offset + 4;
      }
      topic.partitions.push(partition);
    }
    broker.topics.push(topic);
  }

  ret.brokers.push(broker);
  return offset;
};



var decode = function(buf, result, cb) {
  var offset = 8;
  var brokerIdx;
  var brokerCount;

  result.brokers = [];

  brokerCount  = buf.readUInt32BE(offset);
  offset = offset + 4;
  for (brokerIdx = 0; brokerIdx < brokerCount; ++brokerIdx) {
    offset = brokerDecode(buf, offset, result);
  }
  cb(null, result);
};



module.exports = function(callback) {
  return {
    decode: decode,
    callback: callback
  };
};


