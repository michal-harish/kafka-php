<?php 

require_once __DIR__ . "/../../../src/Kafka/Kafka.php";
require_once __DIR__ . "/../../../src/Kafka/V07/ProducerChannel.php";

use Kafka\Kafka;
use Kafka\Message;

class TestV07ProducerChannel extends \Kafka\V07\ProducerChannel {
	public function getStreamContents() {
		rewind($this->socket); return stream_get_contents($this->socket);
	}
	protected function createSocket() {
		if (!is_resource($this->socket)) $this->socket = fopen("php://memory", "rw");
	}
}

//test single message for multiple partitions of one topic without compression
$channel = new TestV07ProducerChannel(new Kafka());
$channel->add(new Message("topic1", 0, "Hello World 1!", Kafka::COMPRESSION_NONE));
$channel->add(new Message("topic1", 1, "Hello World 2!", Kafka::COMPRESSION_NONE));
$channel->add(new Message("topic1", 2, "Hello World 3!", Kafka::COMPRESSION_NONE));
$channel->produce();
assert($channel->getStreamContents() === chr(0).chr(0).chr(0).chr(42).chr(0).chr(0)
	.chr(0).chr(6).chr(116).chr(111).chr(112).chr(105).chr(99).chr(49).chr(0).chr(0)
	.chr(0).chr(0).chr(0).chr(0).chr(0).chr(24).chr(0).chr(0).chr(0).chr(20).chr(1)
	.chr(0).chr(91).chr(4).chr(192).chr(104).chr(72).chr(101).chr(108).chr(108).chr(111)
	.chr(32).chr(87).chr(111).chr(114).chr(108).chr(100).chr(32).chr(49).chr(33).chr(0)
	.chr(0).chr(0).chr(42).chr(0).chr(0).chr(0).chr(6).chr(116).chr(111).chr(112).chr(105)
	.chr(99).chr(49).chr(0).chr(0).chr(0).chr(1).chr(0).chr(0).chr(0).chr(24).chr(0).chr(0)
	.chr(0).chr(20).chr(1).chr(0).chr(112).chr(41).chr(147).chr(171).chr(72).chr(101)
	.chr(108).chr(108).chr(111).chr(32).chr(87).chr(111).chr(114).chr(108).chr(100)
	.chr(32).chr(50).chr(33).chr(0).chr(0).chr(0).chr(42).chr(0).chr(0).chr(0).chr(6)
	.chr(116).chr(111).chr(112).chr(105).chr(99).chr(49).chr(0).chr(0).chr(0).chr(2)
	.chr(0).chr(0).chr(0).chr(24).chr(0).chr(0).chr(0).chr(20).chr(1).chr(0).chr(105)
	.chr(50).chr(162).chr(234).chr(72).chr(101).chr(108).chr(108).chr(111).chr(32)
	.chr(87).chr(111).chr(114).chr(108).chr(100).chr(32).chr(51).chr(33));

//test single message set compressed with gzip 
$channel = new TestV07ProducerChannel(new Kafka());
$channel->add(new Message("topic1", 0, "Hello World! A", Kafka::COMPRESSION_GZIP));
$channel->add(new Message("topic1", 0, "Hello World! B", Kafka::COMPRESSION_GZIP));
$channel->add(new Message("topic1", 0, "Hello World! C", Kafka::COMPRESSION_GZIP));
$channel->produce();
assert($channel->getStreamContents() === chr(0).chr(0).chr(0).chr(91).chr(0).chr(0).chr(0)
	.chr(6).chr(116).chr(111).chr(112).chr(105).chr(99).chr(49).chr(0).chr(0).chr(0).chr(0)
	.chr(0).chr(0).chr(0).chr(73).chr(0).chr(0).chr(0).chr(69).chr(1).chr(1).chr(60).chr(246)
	.chr(98).chr(88).chr(31).chr(139).chr(8).chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(3)
	.chr(99).chr(96).chr(96).chr(16).chr(97).chr(100).chr(112).chr(89).chr(251).chr(66).chr(220)
	.chr(35).chr(53).chr(39).chr(39).chr(95).chr(33).chr(60).chr(191).chr(40).chr(39).chr(69)
	.chr(81).chr(193).chr(145).chr(1).chr(44).chr(126).chr(119).chr(201).chr(206).chr(181)
	.chr(40).chr(226).chr(78).chr(16).chr(241).chr(85).chr(139).chr(59).chr(173).chr(81)
	.chr(196).chr(157).chr(1).chr(143).chr(150).chr(159).chr(232).chr(72).chr(0).chr(0).chr(0));

//test uncompressed message followed by a message set compressed with gzip
$channel = new TestV07ProducerChannel(new Kafka());
$channel->add(new Message("topic1", 0, "Hello World!", Kafka::COMPRESSION_NONE));
$channel->add(new Message("topic1", 0, "Hello World! A", Kafka::COMPRESSION_GZIP));
$channel->add(new Message("topic1", 0, "Hello World! B", Kafka::COMPRESSION_GZIP));
$channel->add(new Message("topic1", 0, "Hello World! C", Kafka::COMPRESSION_GZIP));
$channel->produce();
assert($channel->getStreamContents() === chr(0).chr(0).chr(0).chr(113).chr(0).chr(0).chr(0)
	.chr(6).chr(116).chr(111).chr(112).chr(105).chr(99).chr(49).chr(0).chr(0).chr(0).chr(0)
	.chr(0).chr(0).chr(0).chr(95).chr(0).chr(0).chr(0).chr(18).chr(1).chr(0).chr(28).chr(41)
	.chr(28).chr(163).chr(72).chr(101).chr(108).chr(108).chr(111).chr(32).chr(87).chr(111)
	.chr(114).chr(108).chr(100).chr(33).chr(0).chr(0).chr(0).chr(69).chr(1).chr(1).chr(60)
	.chr(246).chr(98).chr(88).chr(31).chr(139).chr(8).chr(0).chr(0).chr(0).chr(0).chr(0)
	.chr(0).chr(3).chr(99).chr(96).chr(96).chr(16).chr(97).chr(100).chr(112).chr(89).chr(251)
	.chr(66).chr(220).chr(35).chr(53).chr(39).chr(39).chr(95).chr(33).chr(60).chr(191).chr(40)
	.chr(39).chr(69).chr(81).chr(193).chr(145).chr(1).chr(44).chr(126).chr(119).chr(201)
	.chr(206).chr(181).chr(40).chr(226).chr(78).chr(16).chr(241).chr(85).chr(139).chr(59)
	.chr(173).chr(81).chr(196).chr(157).chr(1).chr(143).chr(150).chr(159).chr(232).chr(72)
	.chr(0).chr(0).chr(0));
