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


describe('fetch test', function(){

  beforeEach(function(done) {
    var options = {
      host: 'localhost',
      port: 9094,
      clientId: 'fish',
      maxBytes: 2000000
    };
    client = require('../../lib/api')(options);
    client.tearUp(function(err) {
      console.log(err);
      done();
    });
  });


  it('should connect to Kafka and execute a fetch request', function(done){
    client.fetch({topic: 'testing', partition: 0, offset: 0, maxWait: 0, minBytes: 0}, function(err, response) {
      console.dir(response);
      assert(null === err);
      assert.equal(response.topic, 'testing');
      done();
    });
  });
});


