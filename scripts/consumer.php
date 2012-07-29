<?php
//bootstrap 
chdir(dirname(__FILE__));
require "../lib/Kafka/Kafka.php";

//default properties
$kafkaHost = 'localhost';
$kafkaPort = 9092;
$topic = NULL;
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
            $topic = $arg;
        break;
    }
}
if (!$topic)
{
    exit("\nUsage: php consumer.php <topicname> [--offset <hex_offset>] [--broker <kafka_host:kafka_port>]\n\n");
}

//create connection and do offsets request and fetch request
$kafka = new Kafka($kafkaHost, $kafkaPort);

$consumer = $kafka->createConsumer();
echo "\nOFFSETS REQUEST\n\n";
foreach($consumer->offsets($topic, 0) as $offset )
{
    echo $offset . "\n";
}
echo "\nFETCH REQUEST\n";

if ($consumer->fetch($topic, 0, new Kafka_Offset($offsetHex)))
{
	//while(true)
	{
	    while ($message = $consumer->nextMessage())
	    {
	        echo "\n[" . $message->offset() . "] " . $message->payload();
	    }
	    //usleep(250);
	}
	
	echo "\nNo more messages - new watermark offset: " . $consumer->getOffset() . "\n\n";
}

//go home
$kafka->close();


