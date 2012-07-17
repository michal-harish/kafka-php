<?php
/**
 * Fetch requests is a wrapper for single topic filter.
 * 
 * @author michal.harish@gmail.com
 *
 */
class Kafka_FetchRequest
{
    //connection object
	private $broker;
	
    //request state attributes
    private $requestSent = FALSE;    
    private $responseSize = NULL;

    //fetch attributes
    private $topicFilter;
    private $offset;
    private $maxSize;

    /**
     * @param Kafka_Broker $broker - Connection object
     * @param Kafka_TopicFilter $topicFilter - Which topic(s) to fetch from
     * @param Kafka_Offset $offset - Offset to fetch messages from
     * @param int $maxSize - Maximum size of a single message
     */
    public function __construct(
    	Kafka_Broker $broker,
        Kafka_TopicFilter $topicFilter,
        Kafka_Offset $offset = NULL, 
        $maxSize = 1000000
    )
    {
    	$this->broker = $broker;
        $this->topicFilter = $topicFilter;
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
     * The main method that pulls for messages
     * to come downstream. If there are no more messages
     * it will turn off the requestSent state and return FALSE
     * so that next time another request is made to the broker
     * if the program wants to check again later.
     * 
     * @throws Kafka_Exception
     * @return Kafka_Message | FLASE if no more messages
     */
    public function nextMessage()
    {
        //check the state of the connection
        if (!$this->requestSent)
        {
            if (!$this->send())
            {
                throw new Kafka_Exception(
                    "Request has not been sent - maybe a connection problem."
                );
            }
        }
        $socket = $this->broker->getSocket();
        //has the response size been read yet ?
        if ($this->responseSize === NULL)
        {
            $this->responseSize = array_shift(unpack('N', fread($socket, 4)));
            //read the errorCode
            $errorCode = array_shift(unpack('n', fread($socket, 2)));
            $this->responseSize -= 2;
        }
        //has the request been read completely ?
        if ($this->responseSize < 0)
        {
            throw new Kafka_Exception(
                "Corrupt response stream!"
            );
        } elseif ($this->responseSize == 0)
        {
            $this->requestSent = FALSE;
            $this->responseSize = NULL;
            return FALSE;
        }
        //we still have messages in the response stream, so read the next one
        $message = Kafka_Message::createFromStream($socket, clone $this->offset);
        //move the fetcher offset behind this message
        $this->offset->addInt($message->size());
        //adjust remaining size of the response 
        $this->responseSize -= $message->size();
        //here you are
        return $message;
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

    /**
     * Internal fired directly from the constructor
     * that initiates conversation with Kafka broker.
     * @throws Kafka_Exception
     * @return bool Success
     */
    private function send()
    {
        if ($this->requestSent)
        {
            throw new Kafka_Exception(
                "Previous FetchRequest was not processed fullly."
            );
        }
        //write size of the request
        $requestSize =
            2    // short request key
            + $this->topicFilter->size() // short topic length + topic + int partition
            + $this->offset->size() // long
            + 4 // int
        ;
        $socket = $this->broker->getSocket();
        $written = fwrite($socket, pack('N', $requestSize));
        $written += fwrite($socket, pack('n', Kafka_Broker::REQUEST_KEY_FETCH));
        $written += $this->topicFilter->writeTo($socket);//topic and partition
        $written += $this->offset->writeTo($socket);
        $written += fwrite($socket, pack('N', $this->maxSize));        
        if ($written  != $requestSize + 4)
        {
        	throw new Kafka_Exception(
        		"FetchRequest written $written bytes, expected to send:" . ($requestSize + 4)
        	);
        }

        //change the request state
        $this->requestSent = TRUE;
        $this->responseSize = NULL;
        return TRUE;
    }
}