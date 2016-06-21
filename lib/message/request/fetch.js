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



var offset = function(offsetLo, offsetHi) {
  // THIS should be a 64bit number, but will require some internal work to add
  // that support. so, for now, I am padding a 32 bit integer with 32bits
  // of 0, which will achieve what we need.
  // TODO: FIXME
  this.buf.appendUInt32BE(offsetHi || 0);

  this.buf.appendUInt32BE(offsetLo);
  return this;
};



var maxBytes = function(bytes) {
  this.buf.appendUInt32BE(bytes);
  return this;
};



exports.encode = function(version) {
  var ret = common.encode(common.FETCH_API, version);

  ret.maxWait = maxWait;
  ret.minBytes = minBytes;
  ret.offset = offset;
  ret.maxBytes = maxBytes;
  return ret;
};
