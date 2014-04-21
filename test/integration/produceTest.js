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

var assert  = require('chai').assert;
var client;

/*
 * not leader error
 *
00000000: 0000 0023 1d3d bd15 0000 0001 0007 7465  ...#.==.......te
00000010: 7374 696e 6700 0000 0100 0000 0000 03ff  sting...........
00000020: ffff ffff ffff ff                        .......
*/

describe('produce test', function(){

  beforeEach(function(done) {
    var options = {
      host: 'localhost',
      port: 9092,
      clientId: 'fish'
    };
    client = require('../../lib/api')(options);
    client.tearUp(function(err) {
      console.log(err);
      done();
    });
  });


  it('should connect to Kafka and execute a produce request', function(done){
    client.produce({topic: 'testing123', partition: 0}, ['wotcher mush',
                                                      'orwlight geezer',
                                                      'orwlight geezer',
                                                      'orwlight geezer',
                                                      'orwlight geezer',
                                                      'orwlight geezer',
                                                      'orwlight geezer',
                                                      'orwlight geezer',
                                                      'orwlight geezer',
                                                      'ow do chap'], function(err, response) {
      assert(null === err);
      assert.equal(response.topic, 'testing123');
      done();
    });
  });
});


