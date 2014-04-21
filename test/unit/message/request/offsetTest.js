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
var offset = require('../../../../lib/message/request/offsetFetch');
var hexy = require('hexy');


describe('offset test', function(){

  beforeEach(function(done) {
    done();
  });


  it('should correclty encode an offset request', function(done){
    var msg = offset.encode()
                    .correlation(1234)
                    .client('Mr Flibble')
                    .group('Motorhead')
                    .topic('vole-frobulation')
                    .partition(0)
                    .end();

    var expected = '00000000: 0009 0000 0000 04d2 000a 4d72 2046 6c69  .......R..Mr.Fli\n' +
                   '00000010: 6262 6c65 0009 4d6f 746f 7268 6561 6400  bble..Motorhead.\n' +
                   '00000020: 0000 0100 1076 6f6c 652d 6672 6f62 756c  .....vole-frobul\n' +
                   '00000030: 6174 696f 6e00 0000 0100 0000 00         ation........\n';
    assert.equal(hexy.hexy(msg), expected);
    done();
  });
});

