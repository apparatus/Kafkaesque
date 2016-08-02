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
var leaveGroup = require('../../../../lib/message/request/leaveGroup');
var hexy = require('hexy');


describe('leaveGroup test', function(){

  beforeEach(function(done) {
    done();
  });


  it('should correctly encode a leaveGroup request', function(done){
    var msg = leaveGroup.encode()
                        .correlation(123)
                        .client('testClient')
                        .group('group1')
                        .member('pingpong')
                        .end();

    var expected =  '00000000: 000d 0000 0000 007b 000a 7465 7374 436c  .......{..testCl\n' +
                    '00000010: 6965 6e74 0006 6772 6f75 7031 0008 7069  ient..group1..pi\n' +
                    '00000020: 6e67 706f 6e67                           ngpong\n'

    assert.equal(hexy.hexy(msg), expected);
    done();
  });
});
