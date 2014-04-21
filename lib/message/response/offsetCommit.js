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
  var commit = { hasErrors: false };
  var err = null;
  var s;

  var topicCount = buf.readUInt32BE(offset);
  offset = offset + 4;
  commit.topics = [];
  for (var topicIdx = 0; topicIdx < topicCount; ++topicIdx) {
    var topic = { partitions: [] };

    s = common.readString(buf, offset);
    topic.name = s.str;
    offset = s.offset;

    var partitionCount = buf.readUInt32BE(offset);
    offset = offset + 4;
    for (var partitionIdx = 0; partitionIdx < partitionCount; ++partitionIdx) {
      var partition = {};

      partition.partitionId = buf.readUInt32BE(offset);
      offset = offset + 4;

      partition.error = buf.readUInt16BE(offset);
      if (partition.error !== 0) {
        commit.hasErrors = true;
        err = true;
      }
      offset = offset + 2;

      topic.partitions.push(partition);
    }
    commit.topics.push(topic);
  }
  return {err: err, result: commit};
};



module.exports = function(callback) {
  return {
    decode: decode,
    callback: callback
  };
};

