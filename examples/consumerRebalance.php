<?php

error_reporting(E_ALL | E_STRICT);

// bootstrap
require dirname(__FILE__) . "/../src/Kafka/Kafka.php";

require dirname(__FILE__) . "/../src/Kafka/V07/Metadata.php";

$consumer1 = \Kafka\ConsumerConnector::Create('hq-mharis-d02:2181', 'GroupXYZ');
$context1 = $consumer1->createMessageStreams("test");

//$consumer2 = \Kafka\ConsumerConnector::Create('hq-mharis-d02:2181', 'GroupXYZ');
//$context2 = $consumer2->createMessageStreams("test");

while(true) {
    foreach($context1 as $stream) {
        while ($message = $stream->nextMessage()) {
            echo $message->payload(). "\n";
        }
    }
    sleep(1);
    //break;
}
