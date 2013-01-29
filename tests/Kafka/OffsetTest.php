<?php

require_once __DIR__ . "/../../src/Kafka/Kafka.php";

//test serialization and increment of the offset
$offset = new \Kafka\Offset(dechex("65535"));
$data = $offset->getData();
assert($data === chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(255).chr(255));

$offset->addInt(65535);
assert((string) $offset === "000000000001fffe");

$offset->addInt(4294836225);
assert((string) $offset === "00000000ffffffff");

$offset->add(new \Kafka\Offset("ffffffff00000000"));
assert((string) $offset === "ffffffffffffffff");

$offset2 = new \Kafka\Offset(\Kafka\Kafka::OFFSETS_EARLIEST);
assert((string) $offset2 === "fffffffffffffffe");
$offset2 = new \Kafka\Offset(\Kafka\Kafka::OFFSETS_LATEST);
assert((string) $offset2 === "ffffffffffffffff");
