<?php
/**
 * Kafka Message object used both for producing and conusming messages.
 * Handles format detection from the stream as well as compression/decompression 
 * of the payload and crc validation. 
 * 
 * @author michal.harish@gmail.com
 */

class Kafka_Message
{
	private $offset;		
	private $size;
	private $magic;
	private $compression;
	private $crc32;
	private $payload;
	/**
	 * Constructor is private used by the static creator methods below.
	 * 
	 * @param Kafka_Offset $offset
	 * @param int $size
	 * @param int $magic
	 * @param int $compression
	 * @param int $crc32
	 * @param string $payload
	 * @throws Kafka_Exception
	 */
	private function __construct(
		Kafka_Offset $offset,
		$size,
		$magic,
		$compression,
		$crc32,
		$payload			
	)
	{
		$this->offset = $offset;
		$this->size = $size;
		$this->crc32 = $crc32;
		$this->payload = $payload;	
		if (!$this->isValid())
		{
			throw new Kafka_Exception("Invalid message CRC32");			
		}	
	}
	
	/**
	 * Final value of the uncompressed payload
	 * @return string
	 */
	final public function getPayload()
	{
		return $this->payload;
	}
	
	/**
	 * Final information about the message offset in the broker log.
	 * @return Kafka_Offset
	 */
	final public function getOffset()
	{
		return $this->offset;
	}
	
	/**
	 * The total packet size information, not the payload size.
	 * Payload size can be done simply str_len($message->payload())
	 * @return int
	 */
	public function getSize()
	{
		return $this->size + 4;
	}
	
	/**
	 * Internal validator used by the constructor - could be public but then
	 * would be better to add check whether the validation has been done already.
	 * @throws Kafka_Exception
	 */
	private function isValid()
	{
		switch($this->compression)
		{
			case 0:
				$rawPayload = &$this->payload;
				break;
			case 1:
				$rawPayload = gzencode($this->payload);
				break;
			case 2:
				throw new Kafka_Exception("Snappy compression not implemented");
				break;
			default:
				throw new Kafka_Exception("Unknown kafka compression $compression");
				break;
		}
		return crc32($rawPayload) == $this->crc32;
	}

	/**
	* Creates an instance of a Message from a response stream.
	* @param resource $connection
	*/
	public static function createFromStream($connection, $offset)
	{
		$size = array_shift(unpack('N', fread($connection, 4)));
		//read magic
		$magic = array_shift(unpack('C', fread($connection, 1)));
		//adapt to z wire format
		switch($magic)
		{
			case 0:
				//no compression attribute
				$compression = 0;
				break;
			case 1:
				//read compression attribute
				$compression = array_shift(unpack('C', fread($connection, 1)));
				break;
			default:
				throw new Kafka_Exception(
						"Unknown message format - MAGIC = $magic"
			);
			break;
		}
		//read crc
		$crc32 = array_shift(unpack('N', fread($connection, 4)));
		//load payload depending on type of the compression
		switch($compression)
		{
			case 0:
				//message not compressed, read directly from the connection
				$payload = fread($connection, $size - 6);
				break;
			case 1:
				//gzipped, need to hack around missing gzdecode function
				$tempfile = tempnam(sys_get_temp_dir(), 'kafka_payload_');
				$zp = fopen($tempname, "w");
				stream_copy_to_stream($connection, $zp, $size-6);
				fclose($zp);
				$zp = gzopen($tempfile, "r");
				$payload = gzread($zp);
				fclose($zp);
				unlink($tempname);
				break;
			case 2:
				throw new Kafka_Exception("Snappy compression not implemented");
				break;
			default:
				throw new Kafka_Exception("Unknown kafka compression $compression");
			break;
		}
		return new Kafka_Message(
			$offset,
			$size,
			$magic,
			$compression,
			$crc32,
			$payload
		);
	}	
}