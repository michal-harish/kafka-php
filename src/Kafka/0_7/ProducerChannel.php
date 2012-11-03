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
                $batchMessageData = null;
                $batchCompression = Kafka::COMPRESSION_NONE;                
                $produceData = '';
                foreach($messageSet as $message)
                {                	
                	if ($batchCompression != $message->compression())
                	{          
                		if ($batchCompression != Kafka::COMPRESSION_NONE)
                		{      		
                			$produceData .= $this->batchWrap($topic,$partition,$batchMessageData,$batchCompression);
                			$batchMessageData = null;
                		}                		
                		$batchCompression = $message->compression();
                	}

                	if ($batchCompression != Kafka::COMPRESSION_NONE)
                	{
                		$batchMessageData .= $this->packMessage($message, Kafka::COMPRESSION_NONE);
                	} else {
                    	$produceData .= $this->packMessage($message);
                	}                    
                }
                if ($batchMessageData !== null)
                {           	
	                $produceData .= $this->batchWrap($topic,$partition,$batchMessageData,$batchCompression);    	            
                }

                //0.7 api header
                $data = pack('n', Kafka::REQUEST_KEY_PRODUCE); //short
                $data .= pack('n', strlen($topic)) . $topic; //short string
                $data .= pack('N', $partition); //int                
                //0.7 produce messages
                $data .= pack('N', strlen($produceData));
                $data .= $produceData; //
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
    
    /**
     * Internal for producing batch of consecutive messages with the same compress.codec
     *   
     * @returns A compressed payload representing a batch of messages
     */
    private function batchWrap($topic, $partition, &$batchMessageData, $batchCompression)
    {
    	if ($batchCompression != Kafka::COMPRESSION_NONE)
    	{
    		$batchPayload = $this->packMessage(
   				new Kafka_Message(
					$topic,
					$partition,
					$batchMessageData,
					$batchCompression
   				)
    		);    		
    		return $batchPayload;
    	}
    }
    
    
}