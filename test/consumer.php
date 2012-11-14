<?php
$time = microtime(true);
//bootstrap
chdir(dirname(__FILE__));
require "../src/Kafka.php";

//default properties
$kafkaHost = 'localhost';
$kafkaPort = 9092;
$topic = NULL;
$partition = 0;
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
    	case '--partition':
    		$partition= array_shift($args);
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

//connection
$kafka = new Kafka($kafkaHost, $kafkaPort);

//consumer
$consumer = $kafka->createConsumer();

//offsets request
while (true) {
	try {
		$offsets = $consumer->offsets($topic, $partition);
		break;
	} catch (Kafka_Exception $e)
	{
		echo "\nFailed to read offsets from partition $topic:$partition " . $e->getMessage();
		if (--$partition<0) break;
	}
}
echo "\nOFFSETS REQUEST LATEST `$topic:$partition` = ". $offsets[0] . "\n";
if ($offsetHex === NULL || $offsetHex > $offsets[0])
{
    $offsetHex = $offsets[0];
}

//initialize watermark offset
$watermark = new Kafka_Offset($offsetHex);
//fetch request
$totalProcessed = 0;
echo "\nFETCH REQUEST FROM WATERMARK OFFSET: $watermark\n";
while ($consumer->fetch($topic, 0, $watermark))
{
    try {
        while ($message = $consumer->nextMessage())
        {
            $consistent = TRUE;
            $processed = FALSE;
            do {
                try {
                    //no state has been modified yet, but there can be errors when fetching etc.
                    if (rand(0,100) == 0)
                    {
                        throw new Exception('Simulation of consumer failure in a consistent state.');
                    }
                    /*****************************************************************************
                     * Start processing the message.. */
                    $consistent = FALSE;
                    if (rand(0,200) == 0)
                    {
                        throw new Exception('Simulation of consumer failure - THE ERROR OCCURED WHILE THERE IS AN INCONSISTENT STATE CREATED BY PARTIALLY PROCESSED MESSAGE!');
                    }
                    //if processing went fine, we get the new new watermark, but it's not over yet
                    $watermark = $consumer->getWatermark();
                    /* Now store the watermark somerwhere..
                    ** Only when the new watermark has been stored, we're consistent again
                    ** and we can consider the last message 'processed'
                    ******************************************************************************/
                    $consistent = TRUE;
                    $processed = TRUE;
                    $payload = $message->payload();
                    echo "\n[offset:" . $message->offset() . " watermark: $watermark]" . $message->payload() ;
                } catch (Exception $e) {
                       echo "\nERROR PROCESSING MESSAGE AT OFFSET $watermark: " . $e->getMessage();
                       if (!$consistent)
                       {
                           /******************************************************************************
                           * compensate for the inconsistency, e.g. by reversing what was modified
                           ******************************************************************************/
                           $consistent = TRUE;
                       }
                       echo "\n";
                   }
            } while(!$processed);
               $totalProcessed++;
        }
   } catch (Exception $e) {
       echo "\n\nERROR FETCHING MESSAGE AT OFFSET $watermark: " . $e->getMessage();
       echo $e->getTraceAsString();
       break;
   }
}
echo "\nNO MORE MESSAGES, TOTAL PROCESSED MESSAGES: $totalProcessed, NEW WATERMARK: " . $watermark . "\n\n";

//go home
$consumer->close();

echo "PROCESSING TIME: " . (microtime(true) - $time);
