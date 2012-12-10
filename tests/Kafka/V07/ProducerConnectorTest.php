<?php 

require_once __DIR__ . "/../../../src/Kafka/Kafka.php";

use Kafka\Kafka;
use Kafka\Message;

class TestV07ProducerConnector extends \Kafka\ProducerConnector {

    public function __construct(
        TestV07ProducerChannel $producer1, 
        TestV07ProducerChannel $producer2,
        $compression = \Kafka\Kafka::COMPRESSION_NONE,
        $partitioner = null) {
        $this->partitioner = $partitioner === null ? new \Kafka\Partitioner() : $partitioner;
        $this->compression = $compression;
        $this->topicPartitionMapping = array(
            'topic1' => array(
                0 => array(
                    'broker' => 1,
                    'partition' => 0,
                ),
                1 => array(
                    'broker' => 1,
                    'partition' => 1,
                ),
                2 => array(
                    'broker' => 1,
                    'partition' => 2,
                ),
                3 => array(
                    'broker' => 2,
                    'partition' => 0,
                ),
                4 => array(
                    'broker' => 2,
                    'partition' => 1,
                ),
            )
        );
        $this->brokerMapping = array(
            1 => array( 
            	'name' => 'abc-1353063353941',
                'host' => 'somehost-a',
                'port' => 9092,
            ),
            2 => array( 
            	'name' => 'xyz-1353063353941',
                'host' => 'somehost-b',
                'port' => 9092,
            ),
        );
        $this->producerList = array(
            1 => $producer1,
            2 => $producer2,
        );
    }
}

//test deterministc default partitioner goes to broker1 for given keys
$producer1 = new TestV07ProducerChannel(new Kafka());
$producer2 = new TestV07ProducerChannel(new Kafka());
$producerConnector = new TestV07ProducerConnector($producer1, $producer2);
$producerConnector->addMessage("topic1", "hello 1", 0);
$producerConnector->addMessage("topic1", "hello 1", 1);
$producerConnector->addMessage("topic1", "hello 1", 2);
$producerConnector->addMessage("topic1", "hello 1", 5);
$producerConnector->addMessage("topic1", "hello 1", 6);
$producerConnector->addMessage("topic1", "hello 1", 7);
$producerConnector->addMessage("topic1", "hello 1", 10);
assert($producer2->getStreamContents() === '');
assert($producer1->getStreamContents() === '');
assert($producer2->getMessageQueue() === array());
$messageQueue = $producer1->getMessageQueue();
assert(count($messageQueue) === 1); 
assert(isset($messageQueue['topic1']));
$topic1Queue = $messageQueue['topic1'];
assert(count($topic1Queue[0]) == 3);
assert(count($topic1Queue[1]) == 2);
assert(count($topic1Queue[2]) == 2);
foreach($topic1Queue as $partition => $messages) {
    foreach($messages as $message) {
        assert($message instanceof \Kafka\Message);
        assert($message->partition() == $partition);
        assert($message->payload() === 'hello 1');
        assert($message->compression() == \Kafka\Kafka::COMPRESSION_NONE);
    }
}
$producerConnector->produce();
assert($producer2->getStreamContents() === ''); // nothing should go to the producer 2 
assert($producer1->getStreamContents() > '');

//test deterministic partitioner goes to broker2 for given keys with compression enabled
$producer1 = new TestV07ProducerChannel(new Kafka());
$producer2 = new TestV07ProducerChannel(new Kafka());
$producerConnector = new TestV07ProducerConnector($producer1, $producer2, \Kafka\Kafka::COMPRESSION_GZIP);
$producerConnector->addMessage("topic1", "hello 2", 3);
$producerConnector->addMessage("topic1", "hello 2", 4);
$producerConnector->addMessage("topic1", "hello 2", 8);
$producerConnector->addMessage("topic1", "hello 2", 9);
assert($producer1->getMessageQueue() === array());
assert($producer1->getMessageQueue() === array());
$messageQueue = $producer2->getMessageQueue();
assert(count($messageQueue) === 1);
assert(isset($messageQueue['topic1']));
$topic1Queue = $messageQueue['topic1'];
assert(count($topic1Queue[0]) == 2);
assert(count($topic1Queue[1]) == 2);
foreach($topic1Queue as $partition => $messages) {
    foreach($messages as $message) {
        assert($message instanceof \Kafka\Message);
        assert($message->partition() == $partition);
        assert($message->payload() === 'hello 2');
        assert($message->compression() == \Kafka\Kafka::COMPRESSION_GZIP);
    }
}
$producerConnector->produce();
assert($producer1->getStreamContents() === ''); // nothing should go to the producer 1 
assert($producer2->getStreamContents() === chr(0).chr(0).chr(0).chr(68).chr(0).chr(0).chr(0).chr(6).chr(116).chr(111).chr(112).chr(105)
    .chr(99).chr(49).chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(50).chr(0).chr(0).chr(0).chr(46).chr(1).chr(1).chr(39)
    .chr(126).chr(11).chr(89).chr(31).chr(139).chr(8).chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(3).chr(99).chr(96).chr(96).chr(224)
    .chr(101).chr(100).chr(240).chr(123).chr(150).chr(113).chr(55).chr(35).chr(53).chr(39).chr(39).chr(95).chr(193).chr(136).chr(1)
    .chr(93).chr(0).chr(0).chr(172).chr(130).chr(171).chr(95).chr(34).chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(68).chr(0).chr(0)
    .chr(0).chr(6).chr(116).chr(111).chr(112).chr(105).chr(99).chr(49).chr(0).chr(0).chr(0).chr(1).chr(0).chr(0).chr(0).chr(50).chr(0)
    .chr(0).chr(0).chr(46).chr(1).chr(1).chr(39).chr(126).chr(11).chr(89).chr(31).chr(139).chr(8).chr(0).chr(0).chr(0).chr(0).chr(0)
    .chr(0).chr(3).chr(99).chr(96).chr(96).chr(224).chr(101).chr(100).chr(240).chr(123).chr(150).chr(113).chr(55).chr(35).chr(53)
    .chr(39).chr(39).chr(95).chr(193).chr(136).chr(1).chr(93).chr(0).chr(0).chr(172).chr(130).chr(171).chr(95).chr(34).chr(0).chr(0).chr(0)) ;

//test invalid valid key type for default partitioner
try {
    $producerConnector->addMessage("topic1", "hello 2", "StringKey");
    throw new Exception("Expected \Kafka\Exception with invalid partitioner key type.");
} catch (\Kafka\Exception $e) {
    assert($e->getMessage() === "Default Kafka Partitioner only accepts integer keys" );
}

//test test unknown topic exception
try {
    $producerConnector->addMessage("topicX", "hello", 0);
    throw new Exception("Expected \Kafka\Exception with invalid partitioner key type.");
} catch (\Kafka\Exception $e) {
    assert($e->getMessage() === "Unknown Kafka topic `topicX`" );
}


//TODO test custom partitioner

//TODO test md5 partition unifority of distribution


