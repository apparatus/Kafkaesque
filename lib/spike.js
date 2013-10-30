'use strict';

var net = require('net');
require('buffer');
var hexy = require('hexy');
var port = 9092;
var host = 'localhost';

var socket = net.createConnection(port, host);
console.log('Socket created.');



socket.on('data', function(buf) {
  console.log('DATA: ');
  console.log(hexy.hexy(buf));
});



socket.on('connect', function() {
  console.log('connected: ');

  // request meta data for a topic
  //var buf = new Buffer(32);
  var buf = new Buffer(28);
  //buf.writeUInt32BE(0x001C, 0);
  buf.writeUInt32BE(0x0018, 0);
  buf.writeUInt16BE(0x0003, 4);
  buf.writeUInt16BE(0x0000, 6);
  buf.writeUInt32BE(0xCCCCCCCC, 8); // correlation id
  buf.writeUInt16BE(0x004, 12); // client id
  buf.write('fish', 14);
  buf.writeUInt32BE(0x00000001, 18); // topic count
  buf.writeUInt16BE(0x0004, 22);
  buf.write('test', 24); // topic name

  console.log(hexy.hexy(buf));

  socket.write(buf, function(err){
    console.log('ERR:' + err);
  });
});


socket.on('end', function() {
  console.log('DONE');
});


socket.on('end', function(){
  console.log('END');
});
socket.on('timeout', function(){
  console.log('TIMEOUT');
});
socket.on('drain', function(){
  console.log('DRAIN');
});
socket.on('error', function(err){
  console.log('ERROR:' + err);
});
socket.on('close', function(){
  console.log('CLOSE');
});



/*
 *
RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
  ApiKey => int16
  ApiVersion => int16
  CorrelationId => int32
  ClientId => string
  RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest


API name	 ApiKey Value
ProduceRequest	 0
FetchRequest	 1
OffsetRequest	 2
MetadataRequest	 3
LeaderAndIsrRequest	 4
StopReplicaRequest	 5
OffsetCommitRequest	 6
OffsetFetchRequest	 7
*/
