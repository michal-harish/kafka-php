<?php
//bootstrap 
chdir(dirname(__FILE__));
require "../lib/bootstrap.php";

//read arguments 
$topicName = NULL;
$offsetHex = NULL;
while ($arg = array_shift($_SERVER['argv']))
{
	switch($arg)
	{
		case '--offset':
			$offsetHex = array_shift($_SERVER['argv']);
			break;
		default:
			$topicName = $arg;
			break;		
	}
}
if (!$topicName)
{
	exit("\nUsage: php consumer.php <topicname> [--offset <hex_offset>]\n\n");
}

//create request 
$topic1 = new Kafka_FetchRequest(
	new Kafka_TopicFilter($topicName),
	new Kafka_Offset($offsetHex)
);

//receive and dump messages
while ($message = $topic1->nextMessage())
{
	echo "\n[" . $message->getOffset() . "] " . $message->getPayload();
}
echo "\nNo more messages - new watermark offset: " . $topic1->getOffset() . "\n\n";

//go home
$topic1->close();


