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

var assert = require('assert');
var api = require('./api');


/*
 * fetch - check table if
 * lookup and store the leader and also the offset, read from that point and
 * also store commits
 */


/**
 * main entry point for kafka client
 */
module.exports = function(options) {
  assert(options);
  var _options;
  var _cbt;
  var _api;


  var makeKey = function(params) {
    return params.topic + '_' + params.partition;
  };



  /**
   * fetch data from kafka,
   *
   * If this is the first fetch on this topic / partition then first make a metadata
   * request to determine the leader. Then make an offset request to determine the current 
   * offset, cache this data.  Then make the fetch request...
   */
  var fetch = function(params, cb) {
    var stream = _cbt.get(makeKey(params));
    if (!stream) {


      api.metadata(params.topic, function(err, md) {
        if (!err && md) {

        }
      }

        {
  "size": 110,
  "correlation": 839381598,
  "brokers": [
    {
      "brokerId": 0,
      "host": "localhost",
      "port": 9092,
      "topics": [
        {
          "topicErr": 0,
          "topicName": "testing",
          "partitions": [
            {
              "partitionErr": 0,
              "partitionId": 0,
              "leader": 0,
              "replicas": [
                0
              ],
              "isr": [
                0
              ]
            }
          ]
        },
        {
          "topicErr": 0,
          "topicName": "test",
          "partitions": [
            {
              "partitionErr": 0,
              "partitionId": 0,
              "leader": 0,
              "replicas": [
                0
              ],
              "isr": [
                0
              ]
            }
          ]
        }
      ]
    }
  ]
}







      });

      // do the setup here...

      _cbt.put(makeKey(params), {params: params, cb: cb});
    }

    // make the request
    // handle auto commit / commit as required

    /*
  // register handler against the testing2 topic
  kafkaesque.register({group: 'shoal',
                       topic: 'testing2',
                       partition: 0,
                       commit: 'manual',
                       polling: { maxWait: 2000, minBytes: 10000 }}, processData);
});
*/
  };



  /**
   * construct the kafka client
   */
  var construct = function() {
    _cbt =  require('./cbt')();
    _options = options;
    _api = api(_options);
  };



  /**
   * tearup the connection to kafka
   */
  var tearUp = function(cb) {
    _api.tearUp(cb);
  };



  /**
   * teardown the connection to kafka
   */
  var tearDown = function(cb) {
    _api.tearDown(cb);
  };



  construct();
  return {
    tearUp: tearUp,
    tearDown: tearDown,
  };
};

