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
	private $topic;
	private $partition;
    private $offset;
    private $compression;
    private $payload;
    /**
     * Constructor is private used by the static creator methods below.
     * 
     * @param string $topic
     * @param int $partition
     * @param string $payload
     * @param int $compression
     * @param Kafka_Offset $offset
     * @throws Kafka_Exception
     */
    public function __construct(
    	$topic,
    	$partition,        
        $payload,
    	$compression = Kafka::COMPRESSION_NONE,
        Kafka_Offset $offset = NULL
    )
    {
		if (!$topic)
		{
			throw new Kafka_Exception("Topic name cannot be an empty string.");
		}
		$this->topic = $topic;
		if (!is_numeric($partition) || $partition < 0)
		{
			throw new Kafka_Exception("Partition must be a positive integer or 0.");
		}		
    	$this->topic = $topic;
    	$this->partition = $partition;
    	if ($offset === NULL)
    	{
    		new Kafka_Offset();
    	}
        $this->offset = $offset;
        $this->compression = $compression;
        $this->payload = $payload;
    }

    /**
     * @return string
     */
    final public function topic()
    {
    	return $this->topic;
    }
    
    /**
     * @return partition
     */
    final public function partition()
    {
    	return $this->partition;
    }
    
    /**
     * Final value of the uncompressed payload
     * @return string
     */
    final public function payload()
    {
        return $this->payload;
    }

    /**
     * @return int
     */
    final public function compression()
    {
    	return $this->compression;
    }

    /**
     * Final information about the message offset in the broker log.
     * @return Kafka_Offset
     */
    final public function offset()
    {
        return $this->offset;
    }

    /**
    * Creates an instance of a Message from a response stream.
    * @param resource $stream
    * @param Kafka_Offset $offset
    *
    public static function createFromStream($stream, Kafka_Offset $offset)
    {
        if (!$size = @unpack('N', fread($stream, 4)))
        {
            throw new Kafka_Exception("Invalid Kafka Message size");
        }
        $size = array_shift($size);

        //read magic and load relevant attributes
        if (!$magic = @unpack('C', fread($stream, 1)))
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
                $compression = array_shift(unpack('C', fread($stream, 1)));
                $payloadSize = $size - 6;
                break;
            default:
                throw new Kafka_Exception(
                    "Unknown message format - MAGIC = $magic"
                );
            break;
        }
        //read crc
        $crc32 = array_shift(unpack('N', fread($stream, 4)));

        //load payload depending on type of the compression
        switch($compression)
        {
            case Kafka::COMPRESSION_NONE:
                //message not compressed, read directly from the connection
                $payload = fread($stream, $payloadSize);
                //validate the raw payload
                if (crc32($payload) != $crc32)
                {
                    throw new Kafka_Exception("Invalid message CRC32");
                }
                $compressedPayload = &$payload;
                break;
            case Kafka::COMPRESSION_GZIP:
                //gzip header
                $gzHeader = fread($stream, 10); //[0]gzip signature, [2]method, [3]flags, [4]unix ts, [8]xflg, [9]ostype
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
                    $data = fread($stream, 2);
                    $extralen = array_shift(unpack("v", $data));
                    $extra = fread($stream, $extralen);
                    $gzHeader .= $data . $extra;
                }
                if ($gzflags & 8) // FNAME - zero char terminated string
                {
                    $filename = '';
                    while (($char = fgetc($stream)) && ($char != chr(0))) $filename .= $char;
                    $gzHeader .= $filename . chr(0);
                }
                if ($gzflags & 16) // FCOMMENT - zero char terminated string
                {
                    $comment = '';
                    while (($char = fgetc($stream)) && ($char != chr(0))) $comment .= $char;
                    $gzHeader .= $comment . chr(0);
                }
                if ($gzflags & 2) // FHCRC
                {
                    $data = fread($stream, 2);
                    $hcrc = array_shift(unpack("v", $data));
                    if ($hcrc != (crc32($gzHeader) & 0xffff)) {
                        throw new Kafka_Exception('Invalid GZIP header crc');
                    }
                    $gzHeader .= $data;
                }
                //gzip compressed blocks
                $payloadSize -= strlen($gzHeader);
                $gzData = fread($stream, $payloadSize - 8);
                $gzFooter = fread($stream, 8);
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
                    $innerMessage = self::createFromStream($payloadBuffer, new Kafka_Offset());
                    $payload = $innerMessage->getPayload();
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
            $offset,
            $magic,
            $compression,
            $payload,
            $compressedPayload
        );
        return $result;
    }
    */    
}