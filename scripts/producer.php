<?php
//bootstrap 
chdir(dirname(__FILE__));
require "../lib/bootstrap.php";
$request = new Kafka_ProduceRequest('topic2');

$request->produce(
    array(
        Kafka_Message::create(
            'Hello',
            Kafka_Broker::COMPRESSION_NONE
        ),
        Kafka_Message::create(
            'Hello',
            Kafka_Broker::COMPRESSION_GZIP
        ),
    )
);

$request->close();