<?php
//bootstrap 
chdir(dirname(__FILE__));
require "../lib/Kafka/Kafka.php";
//arguments
$topic = isset($_SERVER['argv'][1]) 
	? $_SERVER['argv'][1] 
	: exit("\nUsage: php producer.php <topic_name>\n\n");
//connection
$conn = new Kafka('localhost', 9092);
//request channel
$request = new Kafka_ProduceRequest($conn, $topic);
//add a few messages
$request->add(Kafka_Message::create(
	'MESSAGE 1 - passed as uncompressed message object',
	Kafka::COMPRESSION_NONE
));
$request->add(Kafka_Message::create(
	'MESSAGE 2 - passed as compressed message object ',
	Kafka::COMPRESSION_GZIP
));
$request->add('MESSAGE 3 - just passed as string, using default compression');
$request->add('MESSAGE 4 - another one passed as string, using default compression');
if ($request->produce('Message 5'))
{
	echo "\nSuccesfully published (as far as sending over socket is concerned right now).\n\n";
}
//go home
$conn->close();