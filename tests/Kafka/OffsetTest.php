<?php
require_once __DIR__ . "/../../src/Kafka/Kafka.php";

//test serialization and increment of the offset of 32-bit offset
$offset = new \Kafka\Offset_32bit(dechex("65535"));
$data = $offset->getData();
assert($data === chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(255).chr(255));

$offset->addInt(65535);
assert((string) $offset === "000000000001fffe");

$offset->addInt(4294836225);
assert((string) $offset === "00000000ffffffff");

$offset->add(new \Kafka\Offset_32bit("ffffffff00000000"));
assert((string) $offset === "ffffffffffffffff");

$offset2 = new \Kafka\Offset_32bit(\Kafka\Kafka::OFFSETS_EARLIEST);
assert((string) $offset2 === "fffffffffffffffe");
$offset2 = new \Kafka\Offset_32bit(\Kafka\Kafka::OFFSETS_LATEST);
assert((string) $offset2 === "ffffffffffffffff");


//test serialization / deserialization and increment of the offset of 64-bit offset
if (PHP_INT_SIZE === 8) {
	$offset32 = new \Kafka\Offset_32bit(dechex("4294836225000"));
	$offset64 = new \Kafka\Offset_64bit();
	$offset64->setData($offset32->getData());
	assert((string)$offset64 === "4294836225000");
	assert($offset32->getData() === $offset64->getData());
}
