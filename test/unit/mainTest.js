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
var envelope = require('../../../../lib/message/request/envelope');
var meta = require('../../../../lib/message/request/metadata');
var hexy = require('hexy');


/*
Run main and mock out req and response with fake kafka server

integration to test against real kafka server


metat data response from test
RESPONSE: 
00000000: 0000 0045 83ed 651a 0000 0001 0000 0000  ...E.me.........
00000010: 0009 6c6f 6361 6c68 6f73 7400 0023 8400  ..localhost..#..
00000020: 0000 0100 0000 0474 6573 7400 0000 0100  .......test.....
00000030: 0000 0000 0000 0000 0000 0000 0100 0000  ................
00000040: 0000 0000 0100 0000 00                   .........
*/

describe('metadata test', function(){

  beforeEach(function(done) {
    done();
  });


  it('should correclty encode a metadata request', function(done){
    var msg = meta.encode()
                  .correlation(1234)
                  .client('Mr Flibble')
                  .topics(['wibble', 'fish']);


    // validate correlation code
    assert.equal(msg.readUInt32BE(4), 1234);

    // validate topic count
    assert.equal(msg.readUInt32BE(20), 2);

    // validate last topic name
    assert.equal(msg.toString('utf8', 34, 38), 'fish');

    done();
  });


  it('should correclty encode and envelope metadata request', function(done){
    var msg = envelope(meta.encode()
                           .correlation(1234)
                           .client('Mr Flibble')
                           .topics(['wibble', 'fish']));


    console.log(hexy.hexy(msg));

    // validate length
    assert.equal(msg.readUInt32BE(0), msg.length - 4);

    // validate correlation code
    assert.equal(msg.readUInt32BE(8), 1234);

    // validate topic count
    assert.equal(msg.readUInt32BE(24), 2);

    // validate last topic name
    assert.equal(msg.toString('utf8', 38, 42), 'fish');

    done();
  });
});

