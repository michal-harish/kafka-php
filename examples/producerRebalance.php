<?php

// bootstrap
require dirname(__FILE__) . "/../src/Kafka/Kafka.php";

require dirname(__FILE__) . "/../src/Kafka/V07/Metadata.php";

$p = \Kafka\ProducerConnector::Create('hq-mharis-d02:2181');

while(true) {
    sleep(2);
    try {
        $p->addMessage('test', 'hello');
        $p->addMessage('test', 'world');
        sleep(2);
        $p->produce();
        echo "Produced 2 messages for test\n";
    } catch (\Kafka\Exception\TopicUnavailable $e) {
        echo "Failed to produce ".($e->getMessage())."\n";
    }
}

