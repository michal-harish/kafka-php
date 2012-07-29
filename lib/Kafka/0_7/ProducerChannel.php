<?php 
/**
 * Kafka 0.7 Producer Channel.
 * 
 * In this version there is no acknowledgement that the message has 
 * been received by the broker so the success is measured only
 * by writing succesfully into the socket.
 * 
 * @author michal.harish@gmail.com
 */

include_once 'Channel.php';

class Kafka_0_7_ProducerChannel extends Kafka_0_7_Channel
implements Kafka_IProducer
{

    /**
     * @var array
     */
    private $messageQueue;

    /**
     * @param Kafka $connection
     */
    public function __construct(Kafka $connection)
    {
        parent::__construct($connection);        
        $this->messageQueue = array();
    }
    
    /**
     * Add a single message to the produce queue.
     *
     * @param Kafka_Message|string $message
     * @return boolean Success
     */
    public function add(Kafka_Message $message)
    {
    	$this->messageQueue
    		[$message->topic()]
    		[$message->partition()]
    		[] = $message;
    }
    
    /**
     * Produce all messages added.
     * @throws Kafka_Exception On Failure
     * @return TRUE On Success
     */
    public function produce()
    {
        //in 0.7 kafka api we can only produce a message set to a single topic-partition only
        //so we'll do each individually.
    	foreach($this->messageQueue as $topic => &$partitions)
    	{
    		foreach($partitions as $partition => &$messageSet)
    		{    	
    			//0.7 kapi header
    			$data = pack('n', Kafka::REQUEST_KEY_PRODUCE); //short
		    	$data .= pack('n', strlen($topic)) . $topic; //short string
		    	$data .= pack('N', $partition); //int		    	
		    	//0.7 produce message set
		    	$messageSetData = '';
		    	foreach($messageSet as $message)
		    	{
		    		$messageSetData .= $this->packMessage($message);		    		
		    	}
		    	$data .= pack('N', strlen($messageSetData));
		    	$data .= $messageSetData; //
			    //in 0.7 kafka api there is no acknowledgement so expectResponse for send is FALSE
		    	if ($this->send($data, FALSE))
		    	{    			
			    	//and therefore as long as the send() is happy we deem the message set to
			    	//have been produced - unset the succesfully sent messageSet			    	
    				unset($partitions[$partition]);
		    	}    			 		
    			unset($messageSet);
    		}
    		unset($partitions);
    	}
        //in 0.7 kafka api we skip any reading from the socket for now
        //because no acknowledgements come from Kafka
        //and we return the success only.
        return TRUE; 
    }
    
    
}