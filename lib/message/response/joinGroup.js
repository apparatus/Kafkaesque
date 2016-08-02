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
  var sync = { hasErrors: false };
  var err = null;
  var s;

  sync.errorCode = buf.readUInt16BE(offset);
  if (sync.errorCode !== 0) {
    sync.hasErrors = true;
    err = sync.errorCode;
    return {err: err, result: group};
  }
  offset = offset + 2;

  sync.generationId = buf.readUInt32BE(offset);
  offset = offset + 4;

  s = common.readString(buf, offset);
  sync.groupProtocol = s.str;
  offset = s.offset;

  s = common.readString(buf, offset);
  sync.leaderId = s.str;
  offset = s.offset;

  s = common.readString(buf, offset);
  sync.memberId = s.str;
  offset = s.offset;

  var membersLength = buf.readUInt32BE(offset);
  offset = offset + 4;
  sync.members = [];
  for (var i = 0; i < membersLength; i++) {
    var member = {};

    s = common.readString(buf, offset);
    member.memberId = s.str;
    offset = s.offset;

    var metadataLength = buf.readUInt32BE(offset);
    offset = offset + 4;

    member.metadata = {};

    member.metadata.version = buf.readUInt16BE(offset);
    offset = offset + 2;

    member.metadata.subscriptions = [];
    var subLength = buf.readUInt32BE(offset);
    offset = offset + 4;

    for(var j = 0; j < subLength; j++){
      s = common.readString(buf, offset);
      member.metadata.subscriptions.push(s.str);
      offset = s.offset;
    }
    var userDataLength = buf.readUInt32BE(offset);
    offset = offset + 4 + userDataLength;

    sync.members.push(member);
  }

  return {err: err, result: sync};
};



module.exports = function(callback) {
  return {
    decode: decode,
    callback: callback
  };
};
