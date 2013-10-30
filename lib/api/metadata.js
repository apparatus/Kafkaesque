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


module.exports = function(options) {
  assert(options);
  var _options;
  var _socket;



  var metadata = function(cb) {
    meta.request();
  };



  var produce = function() {
  };



  var consume = function() {
  };



  /**
   * add default request channel for subsciption and create a unique id for this processes
   * response channel. Pass these to the bus for subscription.
   */
  var construct = function() {
    _options = options;
  };


  var tearUp = function(cb) {
    _socket = net.createConnection(_options.port, _options.host);

    _socket.on('data', function(buf) {
    });

    _socket.on('connect', function() {
      if (cb) {
        cb();
      }
    });

    _socket.on('end', function() {
    });

    _socket.on('timeout', function(){
    });

    _socket.on('drain', function(){
      console.log('DRAIN');
    });

    _socket.on('error', function(err){
      console.log('ERROR');
    });

    _socket.on('close', function(){
      console.log('CLOSE');
    });
  };



  var tearDown = function() {
  };



  construct();
  return {
    tearUp: tearUp,
    tearDown: tearDown
  };
};

