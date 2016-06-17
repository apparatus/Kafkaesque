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


var decodeMember = function (buf, offset) {
  var member = {};
  var s;

  s = common.readString(buf, offset);
  member.memberId = s.string;
  offset = s.offset;

  s = common.readString(buf, offset);
  member.clientId = s.string;
  offset = s.offset;

  s = common.readString(buf, offset);
  member.clientHost = s.string;
  offset = s.offset;

  var metadataLength = buf.readUInt32BE(offset);
  offset = offset + 4 + metadataLength;

  var memberAssignLength = buf.readUInt32BE(offset);
  offset = offset + 4;

  member.memberAssignment = {};

  member.memberAssignment.version = buf.readUInt16BE(offset);
  offset = offset + 2;

  var assignLength = buf.readUInt32BE(offset);
  offset = offset + 4;

  for (var i = 0; i < assignLength; i++){
    member.memberAssignment.partitionAssignment[i] = {};

    s = common.readString(buf, offset);
    member.memberAssignment.partitionAssignment[i].topic = s.string;
    offset = s.offset;

    var partLength = buf.readUInt32BE(offset);
    offset = offset + 4;

    for (var j = 0; j < partLength; j++){
      member.memberAssignment.partitionAssignment[i].partitions[j] = buf.readUInt32BE(offset);
      offset = offset + 4;
    }

    var userDataLength = buf.readUInt32BE(offset);
    offset = offset + 4 + userDataLength;
  }

  return { member: member, offset: offset };
}


var decode = function(buf) {
  var offset = 8;
  var groups = { hasErrors: false };
  var err = null;
  var s;
  groups.groups = [];

  var numGroups = buf.readUInt32BE(offset);
  offset = offset + 4;

  for(var i = 0; i < numGroups; i++) {
    var group = {};
    group.errorCode = buf.readUInt16BE(offset);

    if (group.errorCode !== 0) {
      groups.hasErrors = true;
      group.hasErrors = true;
      err = true;
    }
    offset = offset + 2;

    s = common.readString(buf, offset);
    group.groupId = s.string;
    offset = s.offset;

    s = common.readString(buf, offset);
    group.state = s.string;
    offset = s.offset;

    s = common.readString(buf, offset);
    group.protocolType = s.string;
    offset = s.offset;

    s = common.readString(buf, offset);
    group.protocol = s.string;
    offset = s.offset;

    group.members = []

    var numMembers = buf.readUInt32BE(offset);
    offset = offset + 4;
    for (var j = 0; j < numMembers; j++) {
      s = decodeMember(buf, offset);
      group.members.push(s.member);
      offset = s.offset;
    }

    groups.groups.push(group);
  }

  return {err: err, result: groups};
};


// this will only list the groups which a broker currently manages/coordinates
module.exports = function(callback) {
  return {
    decode: decode,
    callback: callback
  };
};
