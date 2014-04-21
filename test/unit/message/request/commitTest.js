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
var commit = require('../../../../lib/message/request/offsetCommit');
var hexy = require('hexy');


describe('commit test', function(){

  beforeEach(function(done) {
    done();
  });


  it('should correclty encode a commit request', function(done){
    var msg = commit.encode()
                    .correlation(1234)
                    .client('Mr Flibble')
                    .group('Motorhead')
                    .topic('vole-frobulation')
                    .partition(0)
                    .offset(20)
                    .timestamp()
                    .commitMetadata()
                    .end();

    var expected =  '00000000: 0008 0000 0000 04d2 000a 4d72 2046 6c69  .......R..Mr.Fli\n' +
                    '00000010: 6262 6c65 0009 4d6f 746f 7268 6561 6400  bble..Motorhead.\n' +
                    '00000020: 0000 0100 1076 6f6c 652d 6672 6f62 756c  .....vole-frobul\n' +
                    '00000030: 6174 696f 6e00 0000 0100 0000 0000 0000  ation...........\n' +
                    '00000040: 0000 0000 14ff ffff ffff ffff ff00 0a6b  ...............k\n' +
                    '00000050: 6166 6b61 6573 7175 65                   afkaesque\n';

    assert.equal(hexy.hexy(msg), expected);
    done();
  });
});

