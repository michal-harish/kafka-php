<?php
/**
 * Fetch requests to fetch a set of messages.
 * 
 * @author michal.harish@gmail.com
 *
 */

class Kafka_FetchRequest extends Kafka_Request
{

	/**
	 * @var Kafka_Offset
	 */
    private $offset;
    
    /**
     * Maxium allowed size for individual message
     * @var int
     */
    private $maxSize;

    /**
     * @param Kafka $connection 
	 * @param string $topic
	 * @param int $partition 
     * @param Kafka_Offset $offset - Offset to fetch messages from
     * @param int $maxSize - Maximum size of a single message
     */
    public function __construct(
    	Kafka $connection,
		$topic,
		$partition = 0,
        Kafka_Offset $offset = NULL, 
        $maxSize = 1000000
    )
    {
    	parent::__construct($connection, $topic, $partition);

        if ($offset === NULL)
        {
            $this->offset = new Kafka_Offset();
        }
        else
        {
            $this->offset = $offset;
        }
        $this->maxSize = $maxSize;        
    }

    /**
     * Internal fired directly from the hasIncomingData to initiate conversation with Kafka broker
     * in this case sending a fetch request. 
     * @throws Kafka_Exception
     * @return bool Ready-to-read state
     */
    protected function send()
    {
        if (!$this->isWritable())
        {
            throw new Kafka_Exception(
                "Kafka Request channel is not writable, there is data to be read from previous response."
            );
        }
        //write size of the request
        $requestSize = 2 + 2 + strlen($this->topic) + 4 + $this->offset->size() + 4;
        $socket = $this->connection->getSocket();
        $written = fwrite($socket, pack('N', $requestSize));
        $written += fwrite($socket, pack('n', Kafka::REQUEST_KEY_FETCH));
		$written += fwrite($socket, pack('n', strlen($this->topic)));
		$written += fwrite($socket, $this->topic );
		$written += fwrite($socket, pack('N', $this->partition));
        $written += $this->offset->writeTo($socket);
        $written += fwrite($socket, pack('N', $this->maxSize));        
        if ($written  != $requestSize + 4)
        {
        	throw new Kafka_Exception(
        		"FetchRequest written $written bytes, expected to send:" . ($requestSize + 4)
        	);
        }
        //return the ready-to-read state
        return TRUE;
    }

    /**
    * The main method that pulls for messages
    * to come downstream. If there are no more messages
    * it will turn off the readable state and return FALSE
    * so that next time another request is made to the connection
    * if the program wants to check again later.
    *
    * @throws Kafka_Exception
    * @return Kafka_Message | FLASE if no more messages
    */
    public function nextMessage()
    {
    	if ($this->hasIncomingData())
    	{
    		$message = Kafka_Message::createFromStream(
    			$this->connection->getSocket(),
    			clone $this->offset
    		);
    		//move the fetcher offset behind this message
    		$this->offset->addInt($message->size());
    		//adjust remaining size of the response
    		$this->responseSize -= $message->size();
    		//here you are
    		return $message;
    	}
    	else
    	{
    		return FALSE;
    	}
    }
    
    /**
     * The last offset position after connection or reading nextMessage().
     * This value should be used for keeping the consumption state.
     * It is different from the nextMessage()->getOffset() in that
     * it points to the offset "after" that message.
     *
     * @return Kafka_Offset
     */
    public function getOffset()
    {
    	return $this->offset;
    }
}