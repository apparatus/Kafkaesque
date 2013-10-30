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



var maxWait = function(timeInMs) {
  this.buf.appendUInt32BE(0xffffffff);  // replica id
  this.buf.appendUInt32BE(timeInMs);
  return this;
};



var minBytes = function(bytes) {
  this.buf.appendUInt32BE(bytes);
  return this;
};



var topic = function(topic) {
  this.buf.appendUInt32BE(0x00000001);    // un-documented topic count hard set to 1
  common.appendString(this.buf, topic);
  return this;
};



var partition = function(partitionNumber) {
  this.buf.appendUInt32BE(0x00000001);    // un-documented partition count hard set to 1
  this.buf.appendUInt32BE(partitionNumber);
  return this;
};



var offset = function(offsetLow, offsetHigh) {
  this.buf.appendUInt32BE(offsetHigh);
  this.buf.appendUInt32BE(offsetLow);
  return this;
};



var maxBytes = function(bytes) {
  this.buf.appendUInt32BE(bytes);
  return this.buf.get();
};


/*
  def readFrom(buffer: ByteBuffer): FetchRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)
    val replicaId = buffer.getInt
    val maxWait = buffer.getInt
    val minBytes = buffer.getInt
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        val offset = buffer.getLong
        val fetchSize = buffer.getInt
        (TopicAndPartition(topic, partitionId), PartitionFetchInfo(offset, fetchSize))
      })
    })
    FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, Map(pairs:_*))
  }
}
*/





exports.encode = function() {
  var ret = common.encode(common.FETCH_API);

  ret.maxWait = maxWait;
  ret.minBytes = minBytes;
  ret.topic = topic;
  ret.partition = partition;
  ret.offset = offset;
  ret.maxBytes = maxBytes;
  return ret;
};


