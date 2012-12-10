<?php

require_once __DIR__ . "/../../src/Kafka/Kafka.php";


$partitioner = new \Kafka\Partitioner();

//test random partitioning
$result = array();
for($i=0; $i<10; $i++) {
    $result[$partitioner->partition(null, 10)] = true;
}
assert(count($result)>1);

//test module partitioning
assert($partitioner->partition(0, 1) === 0 );
assert($partitioner->partition(1, 1) === 0 );
assert($partitioner->partition(2, 1) === 0 );
assert($partitioner->partition(0, 2) === 0 );
assert($partitioner->partition(1, 2) === 1 );
assert($partitioner->partition(2, 2) === 0 );
assert($partitioner->partition(0, 3) === 0 );
assert($partitioner->partition(1, 3) === 1 );
assert($partitioner->partition(2, 3) === 2 );
assert($partitioner->partition(3, 3) === 0 );
assert($partitioner->partition(4, 3) === 1 );

//test default partitioner non-integer key exceptions
try {
    $partitioner->partition("StringKey", 10);
    throw new Exception("Expected \Kafka\Exception with invalid partitioner key type.");
} catch (\Kafka\Exception $e) {
    assert($e->getMessage() === "Default Kafka Partitioner only accepts integer keys" );
}