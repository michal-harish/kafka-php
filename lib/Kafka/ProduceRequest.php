<?php 
/**
 * ProduceRequest is a connection to a topic which implements
 * multiple ways of publishing single message or set of messages.
 * 
 * Currently there is no acknowledgement that the message has 
 * been received by the broker.
 * 
 * @author michal.harish@gmail.com
 */
class Kafka_ProduceRequest
{
    //connection object
	private $broker;
	
    //publish internals
    private $topic;
    private $partition;
    private $defaultCompression;

    /**
     * @param Kafka_Broker $broker - Connection object
     * @param string $topic - topic name to publish to
     * @param int $partition - broker partition
     * @param int $defaultCompression - compression type for messages passed as simple strings 
     */
    public function __construct(
    	Kafka_Broker $broker,
        $topic,
        $partition = 0,
        $defaultCompression = Kafka_Broker::COMPRESSION_GZIP
    )
    {
    	$this->broker = $broker;
        $this->topic = $topic;
        $this->partition = 0;
        $this->defaultCompression = $defaultCompression;
    }

    /**
     * Publish a single message to the topic of this request.
     * 
     * @param Kafka_Message $message
     */
    public function produce(Kafka_Message $message)
    {
        $messageSet = array($message);
        return $this->produce($messageSet);
    }

    /**
     * Produce a set of messages to the topic of this request.
     * 
     * @param array $messageSet Array containing either Kafka_Message objects or 
     * 	payload strings.
     */
    public function publish(array $messageSet)
    {    	
        $messageSetSize = 0;
        foreach($messageSet as &$message)
        {
        	if (is_string($message))
        	{
        		//it's a payload string instead of object
        		$message = Kafka_Message::create(
        			$message,
        			$this->defaultCompression
        		);
        	}
            $messageSetSize += $message->size();
            unset($message);
        }
        $socket = $this->broker->getSocket();
        $requestSize = 2 + 2 + strlen($this->topic) + 4 + 4 + $messageSetSize;
        $written = fwrite($socket, pack('N', $requestSize));
        $written += fwrite($socket, pack('n', Kafka_Broker::REQUEST_KEY_PRODUCE));
        $written += fwrite($socket, pack('n', strlen($this->topic)));
        $written += fwrite($socket, $this->topic);
        $written += fwrite($socket, pack('N', $this->partition));
        $written += fwrite($socket, pack('N', $messageSetSize)); //
        foreach($messageSet as $message)
        {
            $written += $message->writeTo($socket);
        }
        if ($written  != $requestSize + 4)
        {
        	throw new Kafka_Exception(
        		"ProduceRequest written $written bytes, expected to send:" . ($requestSize + 4)
        	);
        }
    }
}