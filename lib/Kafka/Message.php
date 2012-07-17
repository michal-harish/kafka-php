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
    private $magic;
    private $compression;
    private $payload;
    private $compressedPayload; 
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
        $magic,
        $compression,
        $payload,
        $compressedPayload
    )
    {
        $this->offset = $offset;
        $this->magic = $magic;
        $this->compression = $compression;
        $this->payload = $payload;
        $this->compressedPayload = $compressedPayload;
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
    public function size()
    {
        switch ($this->magic)
        {
            case Kafka_Broker::MAGIC_0:
                return 5 + strlen($this->compressedPayload) + 4;
                break;
            case Kafka_Broker::MAGIC_1:
                return 6 + strlen($this->compressedPayload) + 4;
                break;
        }
    }

    /**
     * Write message packet into a stream (mostly request socket)
     * @param resource $stream
     * @return int $written number of bytes succesfully sent
     */
    public function writeTo($stream)
    {
        $written = fwrite($stream, pack('N', $this->size() - 4)); // message bound size
        $written += fwrite($stream, pack('C', Kafka_Broker::MAGIC_1)); 
        if ($this->magic == Kafka_Broker::MAGIC_1 )
        {
            $written += fwrite($stream, pack('C', $this->compression)); 
        }
        $written += fwrite($stream, pack('N', crc32($this->compressedPayload)));
        $written += fwrite($stream, $this->compressedPayload);
        return $written;
    }

    /**
    * Creates an instance of a Message from a response stream.
    * @param resource $connection
    * @param Kafka_Offset $offset
    */
    public static function createFromStream($connection, Kafka_Offset $offset)
    {
        $size = array_shift(unpack('N', fread($connection, 4)));

        //read magic and load relevant attributes
        switch($magic = array_shift(unpack('C', fread($connection, 1))))
        {
            case Kafka_Broker::MAGIC_0:
                //no compression attribute
                $compression = 0;
                $payloadSize = $size - 5;
                break;
            case Kafka_Broker::MAGIC_1:
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
            case Kafka_Broker::COMPRESSION_NONE:
                //message not compressed, read directly from the connection
                $payload = fread($connection, $payloadSize);
                //validate the raw payload
                if (crc32($payload) != $crc32)
                {
                    throw new Kafka_Exception("Invalid message CRC32");
                }
                $compressedPayload = &$payload;
                break;
            case Kafka_Broker::COMPRESSION_GZIP:
                //gzip header
                $gzHeader = fread($connection, 10); //[0]gzip signature, [2]method, [3]flags, [4]unix ts, [8]xflg, [9]ostype
                if (strcmp(substr($gzHeader,0,2),"\x1f\x8b"))
                {
                    throw new Kafka_Exception('Not GZIP format');
                }
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
                    case 1: //compress
                        //TODO have not tested compress method
                        $uncompressedSize = fwrite($payloadBuffer, gzuncompress($gzData));
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
                rewind($payloadBuffer);
                $innerMessage = self::createFromStream($payloadBuffer, new Kafka_Offset());
                fclose($payloadBuffer);
                $payload = $innerMessage->getPayload();
            break;
            case Kafka_Broker::COMPRESSION_SNAPPY:
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

    public static function create($payload, $compression = Kafka_Broker::COMPRESSION_GZIP)
    {
        switch($compression)
        {
            case Kafka_Broker::COMPRESSION_NONE: 
                $compressedPayload = &$payload; 
                break;
            case Kafka_Broker::COMPRESSION_GZIP:
                //Wrap payload as a non-compressed kafka message.
                //This is probably a bug in Kafka where
                //the bytearray passed to compression util contains
                //the message header. 
                $innerMessage = self::create($payload, Kafka_Broker::COMPRESSION_NONE);
                $wrappedPayload = fopen('php://temp', 'wr');
                $innerMessage->writeTo($wrappedPayload);
                rewind($wrappedPayload);
                //gzip the wrappedPayload
                $compressedPayload = gzencode(stream_get_contents($wrappedPayload));
                break;
            case Kafka_Broker::COMPRESSION_SNAPPY:
                throw new Kafka_Exception("Snappy compression not yet implemented in php client");
                break;
            default:
                throw new Kafka_Exception("Unknown kafka compression $compression");
                break;
        }
        return new Kafka_Message(
            new Kafka_Offset(),
            Kafka_Broker::MAGIC_1,
            $compression,
            $payload,
            $compressedPayload
        );
    }
}