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
  var groups = { hasErrors: false };
  var err = null;
  var s;

  groups.errorCode = buf.readUInt16BE(offset);
  if (groups.errorCode !== 0) {
    groups.hasErrors = true;
    err = groups.errorCode;
  }
  offset = offset + 2;

  groups.groups = [];

  var groupsLength = buf.readUInt32BE(offset);
  offset = offset + 4;

  for (var i = 0; i < groupsLength; i++) {
    var group = {};
    s = common.readString(buf, offset)
    group.groupId = s.string;
    offset = s.offset;

    s = common.readString(buf, offset);
    group.protocol = s.string;
    offset = s.offset;
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
