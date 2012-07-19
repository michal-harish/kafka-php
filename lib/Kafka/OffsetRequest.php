<?php
/**
 * Offsets requests to fetch a set of earliest/latest offsets
 * as per kafka log modification.
 * 
 * @author michal.harish@gmail.com
 *
 */

class Kafka_OffsetRequest extends Kafka_Request
{

	/**
	 * Time will be serialized as 64-bit unix timestamp
	 * @var int 
	 */
	private $time = 0;
	
	/**
	 * @var int
	 */
	private $maxNumOffsets = 0;	
	
	/**
	* @param Kafka $connection 
	* @param string $topic
	* @param int $partition 
	* @param int $time - time since
	* @param int $maxNumOffsets - maximum number of offset ro be returned
	*/
	public function __construct(
		Kafka $connection,
		$topic,
		$partition = 0,
		$time = Kafka::OFFSETS_LATEST,		
		$maxNumOffsets = 2
	)
	{
		parent::__construct($connection, $topic, $partition);
		$this->time = $time;
		$this->maxNumOffsets = $maxNumOffsets;
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
		$requestSize = 2 + 2 + strlen($this->topic) + 4 + 8 +4;
		$socket = $this->connection->getSocket();
		$written = fwrite($socket, pack('N', $requestSize));
		$written += fwrite($socket, pack('n', Kafka::REQUEST_KEY_OFFSETS));
		$written += fwrite($socket, pack('n', strlen($this->topic)));
		$written += fwrite($socket, $this->topic);
		$written += fwrite($socket, pack('N', $this->partition));
		if (is_string($this->time))
		{
			//convert hex constant to a long offset
			$offset = new Kafka_Offset($this->time);
		}
		else
		{	//make 64-bit unix timestamp offset
			$offset = new Kafka_Offset();
			for($i=0; $i<1000 * 1; $i++) 
			{
				$offset->addInt($this->time);
			}
		}
		$written += $offset->writeTo($socket);
		$written += fwrite($socket, pack('N', $this->maxNumOffsets));
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
	 * Read the list of offsets
	 * @throws Kafka_Exception
	 * @return array|FALSE
	 */
	public function getOffsets()
	{
		if ($this->hasIncomingData())
    	{			
    		$socket = $this->connection->getSocket();
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
    	} 
    	else
		{			
			return FALSE;
		}
		
	}
}