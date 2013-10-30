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
var client;


describe('metadata test', function(){

  beforeEach(function(done) {
    this.timeout(1000000);
    var options = {
      host: 'localhost',
      port: 9092,
      clientId: 'fish'
    };
    client = require('../../lib/main')(options);
    client.tearUp(function(err) {
      console.log(err);
      done();
    });
  });


  it('should connect to Kafka and execute a metadata request', function(done){
    this.timeout(1000000);
    client.metadata(['test'], function(response) {
      console.log(response);
      done();
    });

    /*
    // validate length
    assert.equal(msg.readUInt32BE(0), msg.length - 4);

    // validate correlation code
    assert.equal(msg.readUInt32BE(8), 1234);

    // validate topic count
    assert.equal(msg.readUInt32BE(24), 2);

    // validate last topic name
    assert.equal(msg.toString('utf8', 38, 42), 'fish');
    */

  });
});

