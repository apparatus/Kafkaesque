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


describe('metadata test', function(){

  beforeEach(function(done) {
    done();
  });


  it('should correclty encode a metadata request', function(done){
    var msg = meta.encode()
                  .correlation(1234)
                  .client('Mr Flibble')
                  .topics(['wibble', 'fish'])
                  .end();

    var expected = '00000000: 0003 0000 0000 04d2 000a 4d72 2046 6c69  .......R..Mr.Fli\n' +
                   '00000010: 6262 6c65 0000 0002 0006 7769 6262 6c65  bble......wibble\n' +
                   '00000020: 0004 6669 7368                           ..fish\n';

    assert.equal(hexy.hexy(msg), expected);
    done();
  });


  it('should correclty encode and envelope metadata request', function(done){
    var msg = envelope(meta.encode()
                           .correlation(1234)
                           .client('Mr Flibble')
                           .topics(['wibble', 'fish'])
                           .end());

    var expected = '00000000: 0000 0026 0003 0000 0000 04d2 000a 4d72  ...&.......R..Mr\n' +
                   '00000010: 2046 6c69 6262 6c65 0000 0002 0006 7769  .Flibble......wi\n' +
                   '00000020: 6262 6c65 0004 6669 7368                 bble..fish\n';

    assert.equal(hexy.hexy(msg), expected);
    done();
  });
});

