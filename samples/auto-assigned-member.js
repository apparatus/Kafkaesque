var kafkaesque = require('kafkaesque');

// use consumer defaults from kafka
// will join kafkaesqueGroup group with client id 'kafkaesque' + Math.floor(Math.random() * 100000000)
// will connect to default broker {host: 'localhost', port: 9092}
var consumer = kafkaesque();

// auto assigned group members should subscribe to the topics they want/need to
// to fetch from.
consumer.subscribe('testing');

consumer.connect(function(err, kafka) {
  if (err) return console.log('ERROR CONNECTING:', err)

  kafka.on('message', function(message, commit) {
    // handle message, and commit when done
    console.log('received msg:', message.value);
    commit();
  });

  kafka.on('electedLeader', function () {
    console.log('now the leader');
  })

  kafka.on('rebalance.start', function () {
    console.log('rebalance started!');
  });

  kafka.on('rebalance.end', function () {
    console.log('rebalance ended!');
  });

  kafka.on('error', function (err) {
    console.log('error causing disconnect', err)
    consumer.disconnect(); // <--- DISCONNECT FROM ENTIRE CLUSTER
  });

  // debug info
  kafka.on('debug', function (info) {
    console.log('debug:', info);
  });

  // called on cluster connection
  kafka.on('connect', function () {
    console.log('connected!');
  });
});
