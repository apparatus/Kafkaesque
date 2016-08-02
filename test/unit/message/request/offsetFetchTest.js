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
var offsetFetch = require('../../../../lib/message/request/offsetFetch');
var hexy = require('hexy');


describe('offsetFetch test', function(){

  beforeEach(function(done) {
    done();
  });

  it('should correctly encode a offsetFetch request for fetching offset from zookeeper', function(done){
    var msg = offsetFetch.encode(0)
                         .correlation(123)
                         .client('test-client')
                         .group('groupfetch')
                         .topic('a-cool-topic')
                         .partition(1)
                         .end();

    var expected =  '00000000: 0009 0000 0000 007b 000b 7465 7374 2d63  .......{..test-c\n' +
                    '00000010: 6c69 656e 7400 0a67 726f 7570 6665 7463  lient..groupfetc\n' +
                    '00000020: 6800 0000 0100 0c61 2d63 6f6f 6c2d 746f  h......a-cool-to\n' +
                    '00000030: 7069 6300 0000 0100 0000 01              pic........\n'

    assert.equal(hexy.hexy(msg), expected);
    done();
  });

  it('should correctly encode a offsetFetch request for fetching offset from coordinator', function(done){
    var msg = offsetFetch.encode(1)
                         .correlation(123)
                         .client('test-client')
                         .group('groupfetch')
                         .topic('a-cool-topic')
                         .partition(1)
                         .end();

    var expected =  '00000000: 0009 0001 0000 007b 000b 7465 7374 2d63  .......{..test-c\n' +
                    '00000010: 6c69 656e 7400 0a67 726f 7570 6665 7463  lient..groupfetc\n' +
                    '00000020: 6800 0000 0100 0c61 2d63 6f6f 6c2d 746f  h......a-cool-to\n' +
                    '00000030: 7069 6300 0000 0100 0000 01              pic........\n'

    assert.equal(hexy.hexy(msg), expected);
    done();
  });
});
