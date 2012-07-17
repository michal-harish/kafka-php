<?php
//bootstrap 
chdir(dirname(__FILE__));
require "../lib/bootstrap.php";

$topicName = isset($_SERVER['argv'][1]) 
	? $_SERVER['argv'][1] 
	: exit("\nUsage: php producer.php <topic_name>\n\n");

$conn = new Kafka_Broker('localhost', 9092);
$request = new Kafka_ProduceRequest($conn, $topicName);

$request->publish(
    array(
        Kafka_Message::create(
            'MESSAGE 1 - passed as uncompressed message object',
            Kafka_Broker::COMPRESSION_NONE
        ),
        Kafka_Message::create(
            'MESSAGE 2 - passed as compressed message object ',
            Kafka_Broker::COMPRESSION_GZIP
        ),
        'MESSAGE 3 - just passed as string, using default compression',
        'MESSAGE 4 - another one passed as string, using default compression',
    )
);

$conn->close();