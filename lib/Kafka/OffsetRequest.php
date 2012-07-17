<?php
/**
 * 
 * 
 * @author michal.harish@gmail.com
 *
 */
class Kafka_OffsetRequest
{
    //connection object
	private $broker;
	
	//request state attributes
	private $requestSent = FALSE;
	private $responseSize = NULL;
	
	//request attributes
	/**
	 * @var string
	 */
	private $topic;
	private $partition = 0;
	private $time = 0;
	private $maxNumOffsets = 0;	
	
	/**
	* @param Kafka_Broker $broker - Connection object
	* @param string $topic - topic name to publish to
	* @param int $maxNumOffsets - maximum number of offset ro be returned
	* @param int $time - time since
	* @param int $partition - broker partition
	*/
	public function __construct(
		Kafka_Broker $broker,
		$topic,
		$partition = 0,
		$time = Kafka_Broker::OFFSETS_LATEST,		
		$maxNumOffsets = 2
	)
	{
		$this->broker = $broker;
		$this->topic = $topic;
		$this->partition = $partition;
		$this->time = $time;
		$this->maxNumOffsets = $maxNumOffsets;
	}
	
	/**
	 * Read the list of offsets
	 * @throws Kafka_Exception
	 * @return array
	 */
	public function getOffsets()
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
		$offsetsLength = array_shift(unpack('N', fread($socket, 4)));
		$this->responseSize -= 4;
		if ($offsetsLength>0)
		{
			$offsets = array_fill(0, $offsetsLength, NULL);
			for($i=0; $i<$offsetsLength; $i++)
			{
				$offset = Kafka_Offset::createFromStream($socket);
				$offsets[$i] = $offset;
				$this->responseSize -= $offset->size();
			}
			return $offsets;
		}
		else
		{			
			return array();
		}
		
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
		$requestSize = 2 + 2 + strlen($this->topic) + 4 + 8 +4;
		$socket = $this->broker->getSocket();
		$written = fwrite($socket, pack('N', $requestSize));
		$written += fwrite($socket, pack('n', Kafka_Broker::REQUEST_KEY_OFFSETS));		
        $written += fwrite($socket, pack('n', strlen($this->topic)));
        $written += fwrite($socket, $this->topic);	
        $written += fwrite($socket, pack('N', $this->partition));
        if (is_string($this->time))
        {	//convert hex constant to a long offset
        	$offset = new Kafka_Offset($this->time);
        }
        else
        {	//make 64-bit unix timestamp offset
        	$offset = new Kafka_Offset();
        	for($i=0; $i<1000 * 1; $i++) $offset->addInt($this->time);
        }
        $written += $offset->writeTo($socket);
		$written += fwrite($socket, pack('N', $this->maxNumOffsets));	
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