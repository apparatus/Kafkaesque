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
var produce = require('../../../../lib/message/request/produce');
var hexy = require('hexy');


describe('produce test', function(){

  beforeEach(function(done) {
    done();
  });


  it('should correclty encode a produce request', function(done){
    var msg = produce.encode()
                     .correlation(1234)
                     .client('Mr Flibble')
                     .timeout()
                     .topic('test')
                     .partition(1)
                     .messages([{key: '', value: 'Mr Flibble'},
                                {key: '', value: 'Fish/Cheese'},
                                {key: '', value: 'Cheese/Fish'}])
                     .end();

    var expected = '00000000: 0000 0000 0000 04d2 000a 4d72 2046 6c69  .......R..Mr.Fli\n' +
                   '00000010: 6262 6c65 0001 0000 07d0 0000 0001 0004  bble.....P......\n' +
                   '00000020: 7465 7374 0000 0001 0000 0001 0000 006e  test...........n\n' +
                   '00000030: 0000 0000 0000 0000 0000 0018 89f0 bb74  .............p;t\n' +
                   '00000040: 0200 0000 0000 0000 000a 4d72 2046 6c69  ..........Mr.Fli\n' +
                   '00000050: 6262 6c65 0000 0000 0000 0000 0000 0019  bble............\n' +
                   '00000060: 6f28 64d3 0200 0000 0000 0000 000b 4669  o(dS..........Fi\n' +
                   '00000070: 7368 2f43 6865 6573 6500 0000 0000 0000  sh/Cheese.......\n' +
                   '00000080: 0000 0000 19af 1de9 3d02 0000 0000 0000  ...../.i=.......\n' +
                   '00000090: 0000 0b43 6865 6573 652f 4669 7368       ...Cheese/Fish\n';


    assert.equal(hexy.hexy(msg), expected);
    done();
  });


  it('should correclty encode a produce request with a single string', function(done){
    var msg = produce.encode()
                     .correlation(1234)
                     .client('Mr Flibble')
                     .timeout()
                     .topic('test')
                     .partition(1)
                     .messages('oscilating badgers')
                     .end();

    var expected = '00000000: 0000 0000 0000 04d2 000a 4d72 2046 6c69  .......R..Mr.Fli\n' +
                   '00000010: 6262 6c65 0001 0000 07d0 0000 0001 0004  bble.....P......\n' +
                   '00000020: 7465 7374 0000 0001 0000 0001 0000 002c  test...........,\n' +
                   '00000030: 0000 0000 0000 0000 0000 0020 1eab 3a32  .............+:2\n' +
                   '00000040: 0200 0000 0000 0000 0012 6f73 6369 6c61  ..........oscila\n' +
                   '00000050: 7469 6e67 2062 6164 6765 7273            ting.badgers\n';

    assert.equal(hexy.hexy(msg), expected);
    done();
  });
});
