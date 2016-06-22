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
var joinGroup = require('../../../../lib/message/request/joinGroup');
var hexy = require('hexy');


describe('joinGroup test', function(){

  beforeEach(function(done) {
    done();
  });


  it('should correctly encode a joinGroup request with no subscriptions', function(done){
    var msg = joinGroup.encode()
                           .correlation(123)
                           .client('testing')
                           .group('test-group')
                           .sessionTimeout(333)
                           .member('test-group-mem-id')
                           .subscription([])
                           .end();

    var expected =  '00000000: 000b 0000 0000 007b 0007 7465 7374 696e  .......{..testin\n' +
                    '00000010: 6700 0a74 6573 742d 6772 6f75 7000 0001  g..test-group...\n' +
                    '00000020: 4d00 1174 6573 742d 6772 6f75 702d 6d65  M..test-group-me\n' +
                    '00000030: 6d2d 6964 0008 636f 6e73 756d 6572 0000  m-id..consumer..\n' +
                    '00000040: 0001 0005 7261 6e67 6500 0000 0a00 0000  ....range.......\n' +
                    '00000050: 0000 0000 0000 00                        .......\n';


    assert.equal(hexy.hexy(msg), expected);
    done();
  });

  it('should correctly encode a joinGroup request with subscriptions', function(done){
    var msg = joinGroup.encode()
                       .correlation(123)
                       .client('testing')
                       .group('test-group')
                       .sessionTimeout(333)
                       .member('test-group-mem-id')
                       .subscription(['topic1', 'topic2'])
                       .end();

    var expected =  '00000000: 000b 0000 0000 007b 0007 7465 7374 696e  .......{..testin\n' +
                    '00000010: 6700 0a74 6573 742d 6772 6f75 7000 0001  g..test-group...\n' +
                    '00000020: 4d00 1174 6573 742d 6772 6f75 702d 6d65  M..test-group-me\n' +
                    '00000030: 6d2d 6964 0008 636f 6e73 756d 6572 0000  m-id..consumer..\n' +
                    '00000040: 0001 0005 7261 6e67 6500 0000 1a00 0000  ....range.......\n' +
                    '00000050: 0000 0200 0674 6f70 6963 3100 0674 6f70  .....topic1..top\n' +
                    '00000060: 6963 3200 0000 00                        ic2....\n';

    assert.equal(hexy.hexy(msg), expected);
    done();
  });
});
