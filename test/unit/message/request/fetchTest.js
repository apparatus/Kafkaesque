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
var fetch = require('../../../../lib/message/request/fetch');
var hexy = require('hexy');


describe('fetch test', function(){

  beforeEach(function(done) {
    done();
  });


  it('should correclty encode a fetch request', function(done){
    var msg = fetch.encode()
                   .correlation(1234)
                   .client('Mr Flibble')
                   .maxWait(1000)
                   .minBytes(1000)
                   .topic('Owl tesselation')
                   .partition(1)
                   .offset(200)
                   .maxBytes(2000)
                   .end();

    var expected = '00000000: 0001 0000 0000 04d2 000a 4d72 2046 6c69  .......R..Mr.Fli\n' +
                   '00000010: 6262 6c65 ffff ffff 0000 03e8 0000 03e8  bble.......h...h\n' +
                   '00000020: 0000 0001 000f 4f77 6c20 7465 7373 656c  ......Owl.tessel\n' +
                   '00000030: 6174 696f 6e00 0000 0100 0000 0100 0000  ation...........\n' +
                   '00000040: 0000 0000 c800 0007 d0                   ....H...P\n';

    assert.equal(hexy.hexy(msg), expected);
    done();
  });
});

