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


/**
 * assuming a single topic only, decode the produce response
 */
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

  // 8 bytes next offset ignored...

  cb(result.errorCode === 0 ? null : result.errorCode, result);
};



module.exports = function(callback) {
  return {
    decode: decode,
    callback: callback
  };
};

