<?php
//bootstrap 
chdir(dirname(__FILE__));
require "../lib/bootstrap.php";

//default properties
$kafkaHost = 'localhost';
$kafkaPort = 9092;
$topicName = NULL;
$offsetHex = NULL;

//read arguments 
$args = array_slice($_SERVER['argv'],1);
while ($arg = array_shift($args))
{
	switch($arg)
	{
	    case '--broker':
	        $connection = explode(':', array_shift($args));
	        $kafkaHost = array_shift($connection);
	        if ($connection) $kafkaPort = array_shift($connection);
        break;
		case '--offset':
			$offsetHex = array_shift($args);
		break;
		default:
			$topicName = $arg;
		break;		
	}
}
if (!$topicName)
{
	exit("\nUsage: php consumer.php <topicname> [--offset <hex_offset>] [--broker <kafka_host:kafka_port>]\n\n");
}

//create request 
$topic1 = new Kafka_FetchRequest(
	new Kafka_TopicFilter($topicName),
	new Kafka_Offset($offsetHex),
	1000000,
	$kafkaHost,
	$kafkaPort
);

//receive and dump messages
while ($message = $topic1->nextMessage())
{
	echo "\n[" . $message->getOffset() . "] " . $message->getPayload();
}

echo "\nNo more messages - new watermark offset: " . $topic1->getOffset() . "\n\n";

//go home
$topic1->close();


