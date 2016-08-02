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
var offsetCommit = require('../../../../lib/message/request/offsetCommit');
var hexy = require('hexy');


describe('offsetCommit test', function(){

  beforeEach(function(done) {
    done();
  });

  it('should correctly encode a offsetCommit request', function(done){
    var msg = offsetCommit.encode()
                          .correlation(123)
                          .client('testclient')
                          .group('grouply')
                          .topic('a-topic')
                          .partition(1)
                          .offset(123)
                          .meta('metainfo')
                          .end();

    var expected =  '00000000: 0008 0000 0000 007b 000a 7465 7374 636c  .......{..testcl\n' +
                    '00000010: 6965 6e74 0007 6772 6f75 706c 7900 0000  ient..grouply...\n' +
                    '00000020: 0100 0761 2d74 6f70 6963 0000 0001 0000  ...a-topic......\n' +
                    '00000030: 0001 0000 0000 0000 007b 0008 6d65 7461  .........{..meta\n' +
                    '00000040: 696e 666f                                info\n';

    assert.equal(hexy.hexy(msg), expected);
    done();
  });
});
