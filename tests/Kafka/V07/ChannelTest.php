<?php 

require_once __DIR__ . "/../../../src/Kafka/Kafka.php";
require_once __DIR__ . "/../../../src/Kafka/V07/Channel.php";
use Kafka\Offset;
use Kafka\Kafka;
use Kafka\Message;

class TestChannel extends \Kafka\V07\Channel {
	public function __construct() {}
	public function setStreamContents($contents) {
		rewind($this->socket); fwrite($this->socket, $contents); rewind($this->socket); 
	}
	public function getStreamContents() {		 
		rewind($this->socket); return stream_get_contents($this->socket); 
	}	
	protected function createSocket() {
		if (!is_resource($this->socket)) { $this->socket = fopen("php://memory", "rw");}
	}
	public function packMessage2(Message $message, $overrideCompression = null) {
		return $this->packMessage($message, $overrideCompression);
	}
	public function send2($requestData, $expectsResposne = true) {
		return $this->send($requestData, $expectsResposne);
	}
	public function read2($size, $stream = null) {
		return $this->read($size, $stream);
	}
	public function hasIncomingData2() {
		return $this->hasIncomingData();
	}
}

//test uncompressed single message
$channel = new TestChannel();
$message1 = new Message("topic1", 2, "Hello!", Kafka::COMPRESSION_NONE);
$data = $channel->packMessage2($message1);
assert($data === chr(0).chr(0).chr(0).chr(12).chr(1).chr(0).chr(157).chr(42).chr(204).chr(86).chr(72).chr(101).chr(108).chr(108).chr(111).chr(33));

//test gzip compressed single message
$channel = new TestChannel();  
$message2 = new Message("topic1", 2, "Hello!", Kafka::COMPRESSION_GZIP);
$data = $channel->packMessage2($message2);
assert($data === chr(0).chr(0).chr(0).chr(32).chr(1).chr(1).chr(90).chr(49).chr(84).chr(139).chr(31).chr(139).chr(8).chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(3).chr(243).chr(72).chr(205).chr(201).chr(201).chr(87).chr(4).chr(0).chr(86).chr(204).chr(42).chr(157).chr(6).chr(0).chr(0).chr(0));

//test send request 0.7 wire format and can read it back
$channel = new TestChannel();
$requestData = "xyz-request-data";
$channel->send2($requestData, true); //expect response
$data = $channel->getStreamContents();
assert(strlen($data) === strlen($requestData) + 4);
assert($data === chr(0).chr(0).chr(0).chr(strlen($requestData)).$requestData);
//test error code in response throws exception
try {
	$channel->setStreamContents(
			chr(0).chr(0).chr(0).chr(strlen($requestData) + 2) 
			. chr(0) . chr(1) // error code 1
			.$requestData
	);
	$channel->hasIncomingData2();
	throw new Exception("Expected \Kafka\Exception with error code: 1 was not thrown");
} catch (\Kafka\Exception $e) {}

//test valid response with 2-byte error code in header
$channel = new TestChannel();
$requestData = "xyz-request-data";
$channel->send2($requestData, true); //expect response
$channel->setStreamContents(
	chr(0).chr(0).chr(0).chr(strlen($requestData) + 2) 
	. chr(0) . chr(0) // no error code
	.$requestData
);
assert($channel->hasIncomingData2());
assert($requestData === $channel->read2(strlen($requestData)));

//throw new Exception("TODO test message set is compressed with inner messages to the byte level");

//throw new Exception("TODO test loadMessage() can decompress message set correctly");


/*
for($i=0; $i<strlen($data); $i++)
{
	$ch = $data[$i];
	echo 'chr('.ord($ch).').';
}
*/






