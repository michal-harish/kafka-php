<?php 

require_once __DIR__ . "/../../src/Kafka/Kafka.php";

//test message default compression and offset
$message = new \Kafka\Message("topic1", 3, "Hello!");
assert($message->topic() === "topic1");
assert($message->partition() === 3);
assert($message->payload() === "Hello!");
assert($message->compression() === \Kafka\Kafka::COMPRESSION_NONE);
assert($message->offset() == new \Kafka\Offset());

//test message without defaults
$message = new \Kafka\Message(
	"topic2", 4, 
	"Hello Topic2!", 
	\Kafka\Kafka::COMPRESSION_GZIP,
	new \Kafka\Offset("f3fb45")
);
assert($message->topic() === "topic2");
assert($message->partition() === 4);
assert($message->payload() === "Hello Topic2!");
assert($message->compression() === \Kafka\Kafka::COMPRESSION_GZIP);
assert($message->offset() == new \Kafka\Offset("f3fb45"));
