var net = require('net');
var buf = require('buffer');

var port = 9092;
//var port = 1234;
var host = 'localhost';

var socket = net.createConnection(port, host);

console.log('Socket created.');


/*
MetadataResponse => [Broker][TopicMetadata]
  Broker => NodeId Host Port
  NodeId => int32
  Host => string
  Port => int32
  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
  TopicErrorCode => int16
  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
  PartitionErrorCode => int16
  PartitionId => int32
  Leader => int32
  Replicas => [int32]
  Isr => [int32]
*/

socket.on('data', function(buf) {
  var response = {};

  response.size = buf.readUInt32BE(0);
  response.correlationId = buf.readUInt32BE(4);
  response.brokerNodeId = buf.readUInt32BE(8);
  response.hostLen = buf.readUInt16BE(12);
  response.host = buf.toString('utf8', 14, 14 + response.hostLen);
  response.port = buf.readUInt32BE(14 + response.hostLen);

  console.log('response: ' + JSON.stringify(response));

  /*
  size 4 bytes
  
Response => CorrelationId ResponseMessage
CorrelationId => int32
ResponseMessage => MetadataResponse | ProduceRespons

MetadataResponse => [Broker][TopicMetadata]
  Broker => NodeId Host Port
  NodeId => int32
  Host => string
  Port => int32
  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
  TopicErrorCode => int16
  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
  PartitionErrorCode => int16
  PartitionId => int32
  Leader => int32
  Replicas => [int32]
  Isr => [int32]
 */ 

});

// try 32 for string length


socket.on('connect', function() {
  console.log('connected: ');

  // request meta data for a topic
  var buf = new Buffer(32);
  buf.writeUInt32BE(0x001C, 0);
  buf.writeUInt16BE(0x0003, 4);
  buf.writeUInt16BE(0x0000, 6);
  buf.writeUInt32BE(0xCCCCCCCC, 8); // correlation id
  buf.writeUInt16BE(0x0004, 12); // client id
  buf.write('fish', 14) 
  buf.writeUInt32BE(0x00000001, 18); // topic count
  buf.writeUInt16BE(0x0004, 22);
  buf.write('test', 24) // topic name
  console.dir(buf);
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
  console.log('ERROR');
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
