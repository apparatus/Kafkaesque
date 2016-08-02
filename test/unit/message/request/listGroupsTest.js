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
var listGroups = require('../../../../lib/message/request/listGroups');
var hexy = require('hexy');


describe('listGroups test', function(){

  beforeEach(function(done) {
    done();
  });


  it('should correctly encode a listGroups request', function(done){
    var msg = listGroups.encode()
                        .correlation(1234)
                        .client('Mr Flibble')
                        .end();

    var expected = '00000000: 0010 0000 0000 04d2 000a 4d72 2046 6c69  .......R..Mr.Fli\n' +
                   '00000010: 6262 6c65                                bble\n';

    assert.equal(hexy.hexy(msg), expected);
    done();
  });
});
