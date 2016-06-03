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
var kafkaesque = require('../../lib/kafkaesque.js');
// var hexy = require('hexy');


describe('kafkaesque test', function(){
  // for the sake of these tests, I will recreate a fresh producer and consumer
  // for every new test.
  var producer, consumer;

  before(function beforeAll(done) {
    done();
  });

  beforeEach(function beforeEach(done) {
    producer = kafkaesque({
      brokers: [{host: 'localhost', port: 9092}],
      clientId: 'MrFlibble',
      maxBytes: 2000000
    });

    consumer = kafkaesque({
      brokers: [{host: 'localhost', port: 9092}],
      clientId: 'fish',
      maxBytes: 2000000
    });
    done();
  });


  it('should tearUp a connection to kafka', function(done){
    // TODO: test
    assert.equal(1, 1);
    done();
  });

  it('should tearDown a connection to kafka', function(done){
    // TODO: test
    assert.equal(1, 1);
    done();
  });
});
