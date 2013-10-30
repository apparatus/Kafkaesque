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


//?? wierdly appears that stirng lengths are encoded in 6 bytes ?? wtf ??
// requires further investigation of kafka source code to determine cause

var topicBlock = function(buf, offset, ret) {
  var block = {};
  var s;

  block.nodeId = buf.readUInt32BE(offset);
  offset = offset + 4;

  // TODO: seems to be 4 undocumented bytes here ???? - skipping
  offset = offset + 4;

  s = common.readString(buf, offset);
  block.host = s.str;
  offset = s.offset;

  block.port = buf.readUInt32BE(offset);
  offset = offset + 4;

  block.topicErr = buf.readUInt16BE(offset);
  offset = offset + 2;

  // TODO: seems to be another 4 undocumented bytes here ???? - skipping
  offset = offset + 4;

  s = common.readString(buf, offset);
  block.topicName = s.str;
  offset = s.offset;

  block.partitionErr = buf.readUInt16BE(offset);
  offset = offset + 2;

  block.partitionId = buf.readUInt32BE(offset);
  offset = offset + 4;

  block.leader = buf.readUInt32BE(offset);
  offset = offset + 4;

  // array of replicas first 4 are length
  block.replicas = buf.readUInt32BE(offset);
  offset = offset + 4;

  // array of in sync replicas first 4 are length
  block.isr = buf.readUInt32BE(offset);
  offset = offset + 4;

  ret.topics.push(block);
  return offset;
};



var decode = function(buf, result, cb) {
  var offset = 8;
  console.log(result.size);
  result.topics = [];
  while (offset < result.size) {
    offset = topicBlock(buf, offset, result);
  }
  cb(result);
};



module.exports = function(callback) {
  return {
    decode: decode,
    callback: callback
  };
};


