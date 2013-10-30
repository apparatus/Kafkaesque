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



var size = function(buf) {
  return buf.readUInt32BE(0);
};



var correlation = function(buf) {
  return buf.readUInt32BE(4);
};



exports.readBytesAsString = function(buf, offset) {
  var len = buf.readInt32BE(offset);
  var str;
  if (len === -1 || len === 0) {
    str = '';
    len = 0;
  }
  else {
    str = buf.toString('utf8', offset + 4, offset + 4 + len);
  }
  return { str: str, offset: offset + 4 + len };
};



exports.readString = function(buf, offset) {
  var len = buf.readInt16BE(offset);
  var str;
  if (len === -1 || len === 0) {
    str = '';
    len = 0;
  }
  else {
    str = buf.toString('utf8', offset + 2, offset + 2 + len);
  }
  return { str: str, offset: offset + 2 + len };
};



/**
 * Decode response head
 * Common:
 * Size             - 4 bytes
 * CorrelationId    - 4 bytes
 */
exports.decodeHead = function(buf) {
  return { size: size(buf),
           correlation: correlation(buf) };
};

