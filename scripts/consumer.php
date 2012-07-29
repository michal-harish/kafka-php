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
foreach($consumer->offsets($topic, 0) as $offsetItem )
{
    echo $offsetItem . "\n";
}
//initialize watermark offset
$watermark = new Kafka_Offset($offsetHex);
//initialize total processed counter
$processed = 0;
while(TRUE)
{	
	echo "\nFETCH REQUEST FROM WATERMARK OFFSET: $watermark\n";
	if ($consumer->fetch($topic, 0, $watermark))
	{
		try {			
			$consistent = TRUE;
		    while ($message = $consumer->nextMessage())
		    {
		    	//no state has been modified yet, but there can be errors when fetching etc.		    			    	
		    	if (rand(0,20) == 0)
		    	{
		    		throw new Exception('Simulation of consumer failure in a consistent state.');
		    	}
		    	/***********************************
		    	 * Start processing the message.. */
		    	$consistent = FALSE;
		    	if (rand(0,20) == 0)
		    	{
		    		throw new Exception('Simulation of consumer failure - THE ERROR OCCURED WHILE THERE IS AN INCONSISTENT STATE CREATED BY PARTIALLY PROCESSED MESSAGE!');
		    	}		    	
		    	//if processing went fine, we get the new new watermark, but it's not over yet 		    	
		    	$watermark = $consumer->getWatermark();		    			    
		    	/* Now store the watermark somerwhere..
		    	* Only when the new watermark has been stored, we're consistent again
		    	* and we can consider the last message 'processed' 
		    	*******************************/		    	 
		    	$consistent = TRUE;
		    	$processed++;
		    	
		        echo "\n[offset:" . $message->offset() . " watermark: $watermark] " . $message->payload() ;	        
		    }		    
		    echo "\nNO MORE MESSAGES, TOTAL PROCESSED MESSAGES: $processed, NEW WATERMARK: " . $watermark . "\n\n";
		    //if there's no more messages without error then it's the last message in the topic
		    break;
	   	} catch (Exception $e)
	   	{
	   		echo "\nERROR PROCESSING MESSAGE AT OFFSET $watermark: " . $e->getMessage();
	   		if (!$consistent)
	   		{
	   			//compensate for the inconsistency, e.g. by reversing what was changed or else 
	   		}
	   		echo "\n";
	   	}   	
	   	usleep(100);
	}
}	

//go home
$kafka->close();


