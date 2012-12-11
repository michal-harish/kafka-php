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

//test uuid partitioner uniformity of distribution
class TestUuidPartitioner extends \Kafka\Partitioner {
    public function partition($uuid, $numPartitions) {
        $hex = str_replace("-", "", $uuid);
        return crc32($hex) % $numPartitions;
    }
}
$uuids = gzopen(__DIR__ . "/../uuid10000.csv.gz", "r");
$parts = array(0,0,0);
$partitioner = new TestUuidPartitioner();
while ($uuid = gzgets($uuids)) {
    if ($uuid = trim($uuid)) {
        $i = $partitioner->partition($uuid, count($parts));
        $parts[$i] ++;
    }
}
gzclose($uuids);
assert($parts === array(3321, 3237, 3442));
$sum = array_sum($parts);
$avg = $sum / count($parts);
$var = array_reduce($parts, function(&$variance, $item) use($avg) { return $variance += pow($avg -  $item, 2); }) / count($parts);
$std = sqrt($var);
$err = round(100 * $std / $sum, 1);
//echo " AVG = $avg, STD = $std, ERR = % $err ";
assert($sum === 10000);
assert($err <= 1);
