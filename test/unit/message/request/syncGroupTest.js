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
var syncGroup = require('../../../../lib/message/request/syncGroup');
var hexy = require('hexy');


describe('syncGroup test', function(){

  beforeEach(function(done) {
    done();
  });

  it('should correctly encode a syncGroup request with empty assignments', function (done) {
    var msg = syncGroup.encode()
                       .correlation(123)
                       .client('test-client')
                       .group('test-group')
                       .generation(2)
                       .member('test-client-member-id')
                       .groupAssignment([])
                       .end();

    var expected =  '00000000: 000e 0000 0000 007b 000b 7465 7374 2d63  .......{..test-c\n' +
                    '00000010: 6c69 656e 7400 0a74 6573 742d 6772 6f75  lient..test-grou\n' +
                    '00000020: 7000 0000 0200 1574 6573 742d 636c 6965  p......test-clie\n' +
                    '00000030: 6e74 2d6d 656d 6265 722d 6964 0000 0000  nt-member-id....\n';

    assert.equal(hexy.hexy(msg), expected);
    done();
  });

  it('should correctly encode a syncGroup request with assignments', function (done) {
    var msg = syncGroup.encode()
                       .correlation(123)
                       .client('test-client')
                       .group('test-group')
                       .generation(2)
                       .member('test-client-member-id')
                       .groupAssignment([
                         {
                           id: 'test-client-member-id',
                           memberAssignment: {
                             version: 0,
                             assignments: [
                               {
                                 topic: "babaganush",
                                 partitions: [1, 2]
                               },
                               {
                                 topic: "fishlips",
                                 partitions: [1, 2]
                               }
                             ],
                             userData: ''
                           }
                         },
                         {
                           id: 'test-client-member-id-2',
                           memberAssignment: {
                             version: 0,
                             assignments: [
                               {
                                 topic: "fishlips",
                                 partitions: [3]
                               }
                             ],
                             userData: ''
                           }
                         }
                       ])
                       .end();

    var expected =  '00000000: 000e 0000 0000 007b 000b 7465 7374 2d63  .......{..test-c\n' +
                    '00000010: 6c69 656e 7400 0a74 6573 742d 6772 6f75  lient..test-grou\n' +
                    '00000020: 7000 0000 0200 1574 6573 742d 636c 6965  p......test-clie\n' +
                    '00000030: 6e74 2d6d 656d 6265 722d 6964 0000 0002  nt-member-id....\n' +
                    '00000040: 0015 7465 7374 2d63 6c69 656e 742d 6d65  ..test-client-me\n' +
                    '00000050: 6d62 6572 2d69 6400 0000 3800 0000 0000  mber-id...8.....\n' +
                    '00000060: 0200 0a62 6162 6167 616e 7573 6800 0000  ...babaganush...\n' +
                    '00000070: 0200 0000 0100 0000 0200 0866 6973 686c  ...........fishl\n' +
                    '00000080: 6970 7300 0000 0200 0000 0100 0000 0200  ips.............\n' +
                    '00000090: 0000 0000 1774 6573 742d 636c 6965 6e74  .....test-client\n' +
                    '000000a0: 2d6d 656d 6265 722d 6964 2d32 0000 001c  -member-id-2....\n' +
                    '000000b0: 0000 0000 0001 0008 6669 7368 6c69 7073  ........fishlips\n' +
                    '000000c0: 0000 0001 0000 0003 0000 0000            ............\n';

    assert.equal(hexy.hexy(msg), expected);
    done();
  });

});
