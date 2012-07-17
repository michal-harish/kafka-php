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

//create connection 
$broker = new Kafka_Broker($kafkaHost, $kafkaPort);

echo "\nOFFSETS REQUEST\n\n";
//offset request
$offsets = new Kafka_OffsetRequest($broker, $topicName);
foreach($offsets->getOffsets() as $offset )
{
	echo $offset . "\n";
}

echo "\nFETCH REQUEST\n";
//create request 
$fetch = new Kafka_FetchRequest(
	$broker,
    new Kafka_TopicFilter($topicName),
    new Kafka_Offset($offsetHex)
);
//receive and dump messages
//while(true)
{
	while ($message = $fetch->nextMessage())
	{
	    echo "\n[" . $message->getOffset() . "] " . $message->getPayload();
	}
	//usleep(250);
}

echo "\nNo more messages - new watermark offset: " . $fetch->getOffset() . "\n\n";

//go home
$broker->close();


