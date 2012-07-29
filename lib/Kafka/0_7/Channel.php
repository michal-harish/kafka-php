<?php
/**
 * Generic kafka 0.7 response-request channel. 
 * 
 * @author michal.harish@gmail.com
 */

abstract class Kafka_0_7_Channel
{
    /**
     * Connection object 
     * @var Kafka
     */
    private $connection;
    
    /**
     * @var resource
     */
    private $socket;

    /**
     * Request channel state
     * @var boolean
     */
    private $readable;
    
    /**
     * Response of a readable channel
     * @var int
     */
    private $responseSize;

    /**
     * Number of bytes read from response
     * @var int
     */
    private $readBytes;


    /**
     * @param Kafka $connection 
     * @param string $topic
     * @param int $partition 
     */
    public function __construct(Kafka $connection)
    {
        $this->connection = $connection;
        $this->readable = FALSE;        
    }

    /**
     * @return boolean
     */
    public function isReadable()
    {
        return $this->readable;
    }

    /**
     * @return boolean
     */
    public function isWritable()
    {
        return !$this->readable;
    }
    
    
    /**
     * Send a bounded request.
     * @param string $requestData
     * @param boolean $expectsResponse 
     * @throws Kafka_Exception
     */
    final protected function send($requestData, $expectsResposne = TRUE)
    {
    	$this->socket = $this->connection->getSocket();
    	if (!$this->isWritable())
		{
			$this->flushIncomingData();
    	}    	    	
    	$requestSize = strlen($requestData);
    	$written = fwrite($this->socket, pack('N', $requestSize));
    	$written += fwrite($this->socket, $requestData);
    	if ($written  != $requestSize + 4)
    	{
    		throw new Kafka_Exception(
    		    "Request written $written bytes, expected to send:" . ($requestSize + 4)
    		);
    	}
    	$this->readable = $expectsResposne;
    	return TRUE;
    }
    
    /**
     * @param int $size
     * @param resource $stream
     * @throws Kafka_Exception
     */
    final protected function read($size, $stream = NULL)
    {    	
    	if ($stream === NULL)
    	{
	        if (!$this->isReadable())
	        {            
	            throw new Kafka_Exception(
	                "Kafka channel is not readable."
	            );
	        }	        
	        $stream = $this->socket;
    	}
        $result = fread($stream, $size);
        if ($stream === $this->socket)
        {
	    	$this->readBytes += $size;
	    	$this->responseSize -= $size;
        }
        return $result; 
    }

    /**
     * Every response handler has to call this method
     * to validate state of the channel and read
     * standard kafka channel headers.
     * @throws Kafka_Exception
     * @return boolean 
     */
    final protected function hasIncomingData()
    {
    	$this->socket = $this->connection->getSocket();
        //check the state of the connection
        if (!$this->isReadable())
        {            
            throw new Kafka_Exception(
                "Request has not been sent - maybe a connection problem."
            );
            $this->responseSize = NULL;
        }        
        //has the response size been read yet ?       
        if ($this->responseSize === NULL)
        {
            $this->responseSize = array_shift(unpack('N', fread($this->socket, 4)));
            //read the errorCode
            $errorCode = array_shift(unpack('n', $this->read(2)));
            if ($errorCode != 0)
            {
                throw new Kafka_Exception("Kafka response channel error code: $errorCode");
            }
        }
        //has the request been read completely ?
        if ($this->responseSize < 0)
        {
            throw new Kafka_Exception(
                "Corrupt response stream!"
            );
        } elseif ($this->responseSize == 0)
        {
            $this->readable = FALSE;
            $this->responseSize = NULL;
            return FALSE;
        } else 
        {
        	$this->readBytes = 0;
            return TRUE;
        }
    }
    
    protected function flushIncomingData()
    {
    	while($this->responseSize > 0 )
    	{
    		if (!$this->read(min($this->responseSize, 8192)))
    		{
    			break;
    		}
    	}
    	$this->readable = FALSE;
    	$this->responseSize = NULL;
    	$this->readable = FALSE;
    }
    
    /**
     * @return int
     */
    public function getReadBytes()
    {
    	return $this->readBytes;
    }
    
    /**
     * @return int
     */
    public function getRemainingBytes()
    {
    	return $this->readBytes;
    }

    /**
     * Internal method for sending message in a correct kapi-0.7 format.
     * @param Kafka_Message $message
     * @throws Kafka_Exception
     */
    final protected function packMessage(Kafka_Message $message)
    {
    	switch($message->compression())
    	{
    		case Kafka::COMPRESSION_NONE:
    			$compressedPayload = $message->payload();
    			break;
    		case Kafka::COMPRESSION_GZIP:
    			//0.7 kapi uses double wrapped messages for compression.    			    			
    			$innerMessage = new Kafka_Message(
	    			$message->topic(),
	    			$message->partition(),
	    			$message->payload(),
	    			Kafka::COMPRESSION_NONE
    			);
    			//Wrap payload as a non-compressed kafka message.
    			$compressedPayload = gzencode($this->packMessage($innerMessage));
    			break;
    		case Kafka::COMPRESSION_SNAPPY:
    			throw new Kafka_Exception("Snappy compression not yet implemented in php client");
    			break;
    		default:
    			throw new Kafka_Exception("Unknown kafka compression $compression");
    		break;
    	}
    	//for reach message using MAGIC_1 format which includes compression attribute byte
    	$messageBoundsSize = 1 + 1 + 4 + strlen($compressedPayload);
    	$data = pack('N', $messageBoundsSize); //int
    	$data .= pack('C', Kafka::MAGIC_1);//byte
    	$data .= pack('C', $message->compression());//byte
    	$data .= pack('N', crc32($compressedPayload));//int
    	$data .= $compressedPayload;//unbounded string
    	return $data;
    }
    
    /**
     * Internal recursive method for loading a kapi-0.7  formatted message.
     * @param unknown_type $topic
     * @param unknown_type $partition
     * @param Kafka_Offset $offset
     * @param unknown_type $stream
     * @throws Kafka_Exception
     */
    final protected function loadMessage($topic, $partition, Kafka_Offset $offset, $stream = NULL)
    {
    	$this->socket = $this->connection->getSocket();
    	if ($stream === NULL)
    	{
    		$stream = $this->socket;
    	}
    	if (!$size = @unpack('N', $this->read(4, $stream)))
    	{
    		throw new Kafka_Exception("Invalid Kafka Message size");
    	}
    	$size = array_shift($size);    	
    	
    	//read magic and load relevant attributes
    	if (!$magic = @unpack('C', $this->read(1, $stream)))
    	{
    		throw new Kafka_Exception("Invalid Kafka Message");
    	}
    	switch($magic = array_shift($magic))
    	{
    		case Kafka::MAGIC_0:
    			//no compression attribute
    			$compression = Kafka::COMPRESSION_NONE;
    			$payloadSize = $size - 5;
    			break;
    		case Kafka::MAGIC_1:
    			//read compression attribute
    			$compression = array_shift(unpack('C', $this->read(1, $stream)));
    			$payloadSize = $size - 6;
    			break;
    		default:
    			throw new Kafka_Exception(
    	                    "Unknown message format - MAGIC = $magic"
    		);
    		break;
    	}
    	//read crc
    	$crc32 = array_shift(unpack('N', $this->read(4, $stream)));
    	
    	//load payload depending on type of the compression
    	switch($compression)
    	{
    		case Kafka::COMPRESSION_NONE:
    			//message not compressed, read directly from the connection
    			$payload = $this->read($payloadSize, $stream);
    			//validate the raw payload
    			if (crc32($payload) != $crc32)
    			{
    				throw new Kafka_Exception("Invalid message CRC32");
    			}
    			$compressedPayload = &$payload;
    			break;
    		case Kafka::COMPRESSION_GZIP:
    			//gzip header
    			$gzHeader = $this->read(10, $stream); //[0]gzip signature, [2]method, [3]flags, [4]unix ts, [8]xflg, [9]ostype
    			if (strcmp(substr($gzHeader,0,2),"\x1f\x8b"))
    			{
    				throw new Kafka_Exception('Not GZIP format');
    			}
    			$gzmethod = ord($gzHeader[2]);
    			$gzflags = ord($gzHeader[3]);
    			if ($gzflags & 31 != $gzflags) {
    				throw new Kafka_Exception('Invalid GZIP header');
    			}
    			if ($gzflags & 1) // FTEXT
    			{
    				$ascii = TRUE;
    			}
    			if ($gzflags & 4) // FEXTRA
    			{
    				$data = $this->read(2, $stream);
    				$extralen = array_shift(unpack("v", $data));
    				$extra = $this->read($extralen, $stream);
    				$gzHeader .= $data . $extra;
    			}
    			if ($gzflags & 8) // FNAME - zero char terminated string
    			{
    				$filename = '';
    				while (($char = $this->read(1, $stream)) && ($char != chr(0))) $filename .= $char;
    				$gzHeader .= $filename . chr(0);
    			}
    			if ($gzflags & 16) // FCOMMENT - zero char terminated string
    			{
    				$comment = '';
    				while (($char = $this->read(1, $stream)) && ($char != chr(0))) $comment .= $char;
    				$gzHeader .= $comment . chr(0);
    			}
    			if ($gzflags & 2) // FHCRC
    			{
    				$data = $this->read(2, $stream);
    				$hcrc = array_shift(unpack("v", $data));
    				if ($hcrc != (crc32($gzHeader) & 0xffff)) {
    					throw new Kafka_Exception('Invalid GZIP header crc');
    				}
    				$gzHeader .= $data;
    			}
    			//gzip compressed blocks
    			$payloadSize -= strlen($gzHeader);
    			$gzData = $this->read($payloadSize - 8, $stream);
    			$gzFooter = $this->read(8, $stream);
    			$compressedPayload = $gzHeader . $gzData . $gzFooter;
    			//validate the payload
    			if (crc32($compressedPayload ) != $crc32)
    			{
    				throw new Kafka_Exception("Invalid message CRC32");
    			}
    			//uncompress now depending on the method flag
    			$payloadBuffer = fopen('php://temp', 'rw');
    			switch($gzmethod)
    			{
    				case 0: //copy
    					$uncompressedSize = fwrite($payloadBuffer, $gzData);
    					break;
    				case 1: //compress
    					//TODO have not tested compress method
    					$uncompressedSize = fwrite($payloadBuffer, gzuncompress($gzData));
    					break;
    				case 2: //pack
    					throw new Kafka_Exception(
    	                            "GZip method unsupported: $gzmethod pack"
    					);
    					break;
    				case 3: //lhz
    					throw new Kafka_Exception(
    	                            "GZip method unsupported: $gzmethod lhz"
    					);
    					break;
    				case 8: //deflate
    					$uncompressedSize = fwrite($payloadBuffer, gzinflate($gzData));
    					break;
    				default :
    					throw new Kafka_Exception(
    	                            "Unknown GZip method : $gzmethod"
    				);
    				break;
    			}
    			//validate gzip data based on the gzipt footer
    			$datacrc = array_shift(unpack("V",substr($gzFooter, 0, 4)));
    			$datasize = array_shift(unpack("V",substr($gzFooter, 4, 4)));
    			rewind($payloadBuffer);
    			if ($uncompressedSize != $datasize || crc32(stream_get_contents($payloadBuffer)) != $datacrc)
    			{
    				throw new Kafka_Exception(
    	            	"Invalid size or crc of the gzip uncompressed data"
    				);
    			}
    			//now unwrap the inner kafka message
    			//- not sure if this is bug in kafka but the scala code works with message inside the compressed payload
    			try {
    				rewind($payloadBuffer);    
    				$innerMessage = $this->loadMessage($topic, $partition, new Kafka_Offset(), $payloadBuffer);
    				$payload = $innerMessage->payload();
    			} catch (Kafka_Exception $ke)
    			{
    				//invalid inner message - probably producer that doesn't wrap header inside the compressed payload
    				$payload = FALSE;
    			}
    			fclose($payloadBuffer);
    			break;
    		case Kafka::COMPRESSION_SNAPPY:
    			throw new Kafka_Exception("Snappy compression not yet implemented in php client");
    			break;
    		default:
    			throw new Kafka_Exception("Unknown kafka compression $compression");
    		break;
    	}
    	$result =  new Kafka_Message(
    		$topic,
    		$partition,
	    	$payload,
    		$compression,
    		$offset
    	);
    	return $result;
    	 
    }
    
}