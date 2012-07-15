<?php
/**
 * Fetch requests is a wrapper for single consumer socket.
 * Connection properties should be moved out to some Kafka properties object.
 * 
 * @author michal.harish@gmail.com
 *
 */
class Kafka_FetchRequest
{
	//connection properties
	private $host;
	private $port;
	private $timeout;
	
	//connection state internals
	private $connection = NULL;
	private $requestSent = FALSE;	
	private $responseSize = NULL;
	
	//fetch internals
	private $topicFilter;
	private $offset;
	private $maxSize;
	
	/**
	 * @param Kafka_TopicFilter $topicFilter - Which topic(s) to fetch from
	 * @param Kafka_Offset $offset - Offset to fetch messages from
	 * @param int $maxSize - Maximum size of a single message
	 * @param string $host - Kafka broker host
	 * @param int $port - Kafka broker port
	 * @param int $timeout - Kafka broker connection timeout
	 */
	public function __construct(
		Kafka_TopicFilter $topicFilter,
		Kafka_Offset $offset = NULL, 
		$maxSize = 1000000,
		$host = 'localhost',
		$port = 9092,
		$timeout = 5
	)
	{
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
		$this->host = $host;
		$this->port = $port;
		$this->timeout = $timeout;		
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
		//has the response size been read yet ?
		if ($this->responseSize === NULL)
		{
			$this->responseSize = array_shift(unpack('N', fread($this->connection, 4)));
			//read the errorCode
			$errorCode = array_shift(unpack('n', fread($this->connection, 2)));
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
		$message = Kafka_Message::createFromStream($this->connection, clone $this->offset);
		//move the fetcher offset behind this message
		$this->offset->addInt($message->getSize());
		//adjust remaining size of the response 
		$this->responseSize -= $message->getSize();
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
	 * Close the connection. Must be called by the application
	 * but could be added to the __destruct method too. 
	 */
	public function close() {
		if (is_resource($this->connection)) {
			fclose($this->connection);
		}
		$this->requestSent = NULL;
	}
	
	/**
	 * Internal fired directly from the constructor
	 * that initiates conversation with Kafka broker.
	 * @throws Kafka_Exception
	 * @return bool Success
	 */
	private function send()
	{
		$this->connect();
	
		if ($this->requestSent)
		{
			throw new Kafka_Exception(
				"Request is pending for the connection $this->connection."
			);
		}
		//write size of the request
		$requestSize =
			2	// short request key
			+ $this->topicFilter->size() // short topic length + topic + int partition
			+ $this->offset->size() // long
			+ 4 // int
		;
		fwrite($this->connection, pack('N', $requestSize));
		//write short FETCH request key
		fwrite($this->connection, pack('n', 1));
		//write topic and partition
		$this->topicFilter->writeTo($this->connection);
		//write offset long
		$this->offset->writeTo($this->connection);
		//write maxSize Int
		fwrite($this->connection, pack('N', $this->maxSize));
	
		//change connection state
		$this->requestSent = TRUE;
		$this->responseSize = NULL;
		return TRUE;
	}

	/**
 	 * Set up the socket connection if not yet done.
	 * @throws Kafka_Exception
	 */
	private function connect()
	{
		if (!is_resource($this->connection))
		{
			$this->connection = stream_socket_client(
				'tcp://' . $this->host . ':' . $this->port, $errno, $errstr
			);
			if (!$this->connection) {
				throw new Kafka_Exception($errstr, $errno);
			}
			stream_set_timeout($this->connection, $this->timeout);
			//stream_set_read_buffer($this->connection,  65535);
			//stream_set_write_buffer($this->connection, 65535);
			$this->requestSent = FALSE;
		}
	}
}