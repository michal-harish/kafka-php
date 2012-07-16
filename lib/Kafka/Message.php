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
	* Creates an instance of a Message from a response stream.
	* @param resource $connection
	*/
	public static function createFromStream($connection, $offset)
	{
		$size = array_shift(unpack('N', fread($connection, 4)));
		
		//read magic and load relevant attributes
		switch($magic = array_shift(unpack('C', fread($connection, 1))))
		{
			case 0:
				//no compression attribute
				$compression = 0;
				$payloadSize = $size - 5;
				break;
			case 1:
				//read compression attribute
				$compression = array_shift(unpack('C', fread($connection, 1)));
				$payloadSize = $size - 6;
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
				$payload = fread($connection, $payloadSize);
				//validate the raw payload
				if (crc32($payload) != $crc32)
				{
				    throw new Kafka_Exception("Invalid message CRC32");
				}
				break;
			case 1:
			    //gzip header
			    $gzHeader = fread($connection, 10); //[0]magic, [2]method, [3]flags, [4]unix ts, [8]xflg, [9]ostype
			    $gzmethod = ord($gzHeader[2]);
			    $gzflags = ord($gzHeader[3]);
			    $payloadSize -=10;
			    //TODO process the gzflags and read extra fields if necessary
			    if ($gzflags & 1) // FTEXT
			    {
			        //
			    }
			    if ($gzflags & 2) // FHCRC
			    {
			        //
			    }
			    if ($gzflags & 4) // FEXTRA
			    {
			        //
			    }
			    if ($gzflags & 8) // FNAME
			    {
			        //
			    }
			    if ($gzflags & 16) // FCOMMENT
			    {
			        //
			    }
			    //gzip compressed blocks
			    $gzData = fread($connection, $payloadSize - 8);
			    $gzFooter = fread($connection, 8);
			    //validate the payload
		        if (crc32($gzHeader . $gzData . $gzFooter ) != $crc32)
				{
				    throw new Kafka_Exception("Invalid message CRC32");
				}
				//uncompress now depending on the method flag
			    switch($gzmethod)
			    {
			        case 0: //copy
			            $payload = &$gzData;
			        case 1: //compress
			            //TODO have not tested compress method
			            $payload = gzuncompress($gzData);
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
			            $payload = gzinflate($gzData);
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
			    if (strlen($payload) != $datasize || crc32($payload) != $datacrc)
			    {
			        throw new Kafka_Exception(
		                "Invalid size or crc of the gzip uncompressed data"
			        );
			    }
			    //FIXME no idea where the extra 10-bytes come from after inflating
			    //but that's what i get when pulling messages produced by kakfa-console-producer.sh
			    $payload = substr($payload, 10);
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