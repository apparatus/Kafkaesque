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
  var group = { hasErrors: false };
  var err = null;
  var s;

  group.errorCode = buf.readUInt16BE(offset);
  if (group.errorCode !== 0) {
    group.hasErrors = true;
    err = group.errorCode;
  }
  offset = offset + 2;

  group.coordinatorId = buf.readUInt32BE(offset);
  offset = offset + 4;

  s = common.readString(buf, offset);
  group.coordinatorHost = s.str;
  offset = s.offset;

  group.coordinatorPort = buf.readUInt32BE(offset);
  offset = offset + 4;

  return {err: err, result: group};
};



module.exports = function(callback) {
  return {
    decode: decode,
    callback: callback
  };
};
