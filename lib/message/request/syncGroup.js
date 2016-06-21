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
var BufferBuilder = require('buffer-builder');


var generation = function (generationId) {
  this.buf.appendUInt32BE(generationId);

  return this;
}


var member = function (memberId) {
  common.appendString(this.buf, memberId);

  return this;
}

// Example assignments structure...
// [
//   {
//     id: "123-id",
//     memberAssignment: {
//       version: 0
//       assignments: [
//         {
//           topic: "babaganush",
//           partitions: [1, 2]
//         },
//         {
//           topic: "fish",
//           partitions: [1, 2]
//         }
//       ],
//       userData: ''
//     }
//   },
//   {
//     id: "123-id",
//     memberAssignment: {
//       version: 0
//       assignments: [
//         {
//           topic: "babaganush",
//           partitions: [3, 4]
//         },
//         {
//           topic: "fish",
//           partitions: [3]
//         }
//       ],
//       userData: ''
//     }
//   }
// ]
var groupAssignment = function (memberAssignments) {
  var buf = this.buf;
  buf.appendUInt32BE(memberAssignments.length);

  memberAssignments.forEach(function (member) {
    common.appendString(buf, member.id);
    // this.buf.appendUInt16BE(0x0001);
    // MemberAssignment Bytes Buffer
    var memberAssignmentBuf = new BufferBuilder();

    // implement consumer protocol for member assignment buffer
    memberAssignmentBuf.appendUInt16BE(member.memberAssignment.version || 0); // version
    memberAssignmentBuf.appendUInt32BE(member.memberAssignment.assignments.length); //topic assignment length

    member.memberAssignment.assignments.forEach(function (assignment) {
      common.appendString(memberAssignmentBuf, assignment.topic);

      memberAssignmentBuf.appendUInt32BE(assignment.partitions.length);
      assignment.partitions.forEach(function (partition) {
        memberAssignmentBuf.appendUInt32BE(partition);
      });

    });
    memberAssignmentBuf.appendUInt32BE(0); // userData

    buf.appendUInt32BE(memberAssignmentBuf.length);
    buf.appendBuffer(memberAssignmentBuf);
  });
  this.buf = buf;
  return this;
}


exports.encode = function() {
  var ret = common.encode(common.SYNCGROUP_API);

  ret.generation = generation;
  ret.member = member;
  ret.groupAssignment = groupAssignment;

  return ret;
};
