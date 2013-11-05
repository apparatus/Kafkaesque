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

var uuid = require('uuid');
var crc32 = require('buffer-crc32');



module.exports = function() {
  var _table;



  var put = function(obj) {
    var id = uuid.v4();
    var idCrc = crc32.unsigned(id);
    _table[idCrc] = { obj: obj, ts: (new Date()).getTime() };
    return idCrc;
  };



  var get = function(id) {
    var obj = _table[id];
    return obj.obj;
  };



  var remove = function(id) {
    var obj = _table[id];
    delete _table[obj];
    return obj.obj;
  };



  var purge = function(/*ts*/) {
  };



  var construct = function() {
    _table = {};
  };



  construct();
  return {
    put: put,
    get: get,
    remove: remove,
    purge: purge
  };
};

