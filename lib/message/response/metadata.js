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



var decodeBroker = function(buf, offset, ret) {
  var broker = {};
  var s;

  broker.brokerId = buf.readUInt32BE(offset);
  offset = offset + 4;

  s = common.readString(buf, offset);
  broker.host = s.str;
  offset = s.offset;

  broker.port = buf.readUInt32BE(offset);
  offset = offset + 4;

  ret.brokers[broker.brokerId] = broker;
  return offset;
};



var decodeTopic = function(buf, offset, ret) {
  var s;
  var idx;
  var topic = { hasErrors: false };

  topic.topicErr = buf.readUInt16BE(offset);
  if (topic.topicErr !== 0) {
    topic.hasErrors = true;
  }
  offset = offset + 2;

  s = common.readString(buf, offset);
  topic.topicName = s.str;
  offset = s.offset;

  var partitionCount = buf.readUInt32BE(offset);
  offset = offset + 4;
  topic.partitions = {};
  for (var partitionIdx = 0; partitionIdx < partitionCount; ++partitionIdx) {
    var partition = {};

    partition.partitionErr = buf.readUInt16BE(offset);
    if (partition.partitionErr !== 0) {
      topic.hasErrors = true;
    }
    offset = offset + 2;

    partition.partitionId = buf.readUInt32BE(offset);
    offset = offset + 4;

    partition.leaderId = buf.readUInt32BE(offset);
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
    topic.partitions[partition.partitionId] = partition;
  }
  ret.topics[topic.topicName] = topic;

  return offset;
};



var decode = function(buf, result) {
  var offset = 8;
  var brokerIdx;
  var topicIdx;
  var brokerCount;

  result.brokers = {};
  brokerCount  = buf.readUInt32BE(offset);
  offset = offset + 4;
  for (brokerIdx = 0; brokerIdx < brokerCount; ++brokerIdx) {
    offset = decodeBroker(buf, offset, result);
  }

  result.topics = {};
  var topicCount = buf.readUInt32BE(offset);
  offset = offset + 4;
  for (topicIdx = 0; topicIdx < topicCount; ++topicIdx) {
    offset = decodeTopic(buf, offset, result);
  }

  return {err: null, result: result};
};



module.exports = function(callback) {
  return {
    decode: decode,
    callback: callback
  };
};


