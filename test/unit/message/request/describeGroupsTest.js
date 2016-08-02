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
var describeGroup = require('../../../../lib/message/request/describeGroups');
var hexy = require('hexy');


describe('describeGroups test', function(){

  beforeEach(function(done) {
    done();
  });


  it('should correctly encode a describeGroup request', function(done){
    var msg = describeGroup.encode()
                           .correlation(123)
                           .client('testClient')
                           .groups(['group1', 'group2'])
                           .end();

    var expected =  '00000000: 000f 0000 0000 007b 000a 7465 7374 436c  .......{..testCl\n' +
                    '00000010: 6965 6e74 0000 0002 0006 6772 6f75 7031  ient......group1\n' +
                    '00000020: 0006 6772 6f75 7032                      ..group2\n';

    
    assert.equal(hexy.hexy(msg), expected);
    done();
  });
});
