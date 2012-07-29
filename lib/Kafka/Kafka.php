<?php
/**
 * Kafka connection object.
 * Currently connects to a single broker, it can be later on extended to provide an auto-balanced 
 * connection to the cluster of borkers without disrupting the client code.
 *  
 * @author michal.harish@gmail.com
 */

include "Exception.php";
include "Offset.php";
include "Message.php";
include "IProducer.php";
include "IConsumer.php";

class Kafka
{
    const MAGIC_0 = 0; //wire format without compression attribute
    const MAGIC_1 = 1; //wire format with compression attribute

    const REQUEST_KEY_PRODUCE      = 0;
    const REQUEST_KEY_FETCH        = 1;
    const REQUEST_KEY_MULTIFETCH   = 2;
    const REQUEST_KEY_MULTIPRODUCE = 3;
    const REQUEST_KEY_OFFSETS      = 4;

    const COMPRESSION_NONE = 0;
    const COMPRESSION_GZIP = 1;
    const COMPRESSION_SNAPPY = 2;

    const OFFSETS_LATEST = "ffffffffffffffff"; //-1L
    const OFFSETS_EARLIEST = "fffffffffffffffe"; //-2L

    //connection properties
    private $host;
    private $port;
    private $timeout;
	private $kapiVersion;
	
    /**
     * Connection socket.
     * @var resource
     */
    private $socket = NULL;

    /**
     * @param string $host
     * @param int $port
     * @param int $timeout
     * @param int $kapiVersion Kafka API Version
     * 	- the client currently recoginzes difference in the wire
     *    format prior to the version 0.8 and the versioned
     *    requests introduced in 0.8
     */
    public function __construct(
        $host = 'localhost',
        $port = 9092,
        $timeout = 6,
        $kapiVersion = 0.7
    )
    {
        $this->host = $host;
        $this->port = $port;
        $this->timeout = $timeout;       
        //include "IOffsetRequest.php";
        if ($kapiVersion < 0.8)
        {
        	$this->kapiImplementation = "0_7";
        }
        else//if ($kapiVersion < 0.8)
        {
        	$this->kapiImplementation = "0_8";
        }
        include_once "{$this->kapiImplementation}/ProducerChannel.php";
        include_once "{$this->kapiImplementation}/ConsumerChannel.php";
        //include_once "{$this->kapiImplementation}/OffsetRequest.php";        
    }

    /**
     * Set up the socket connection if not yet done.
     * @throws Kafka_Exception
     * @return resource $socket
     */
    public function getSocket()
    {
        if (!is_resource($this->socket))
        {
            $this->socket = stream_socket_client(
                'tcp://' . $this->host . ':' . $this->port, $errno, $errstr
            );
            if (!$this->socket) {
                throw new Kafka_Exception($errstr, $errno);
            }
            stream_set_timeout($this->socket, $this->timeout);
            //stream_set_read_buffer($this->connection,  65535);
            //stream_set_write_buffer($this->connection, 65535);
        }
        return $this->socket;
    }

    /**
     * Close the connection(s). Must be called by the application
     * but could be added to the __destruct method too.
     */
    public function close() {
        if (is_resource($this->socket)) {
            fclose($this->socket);
        }
    }

    /**
     * @return Kafka_IProducer
     */
    public function createProducer()
    {
    	$producerClass = "Kafka_{$this->kapiImplementation}_ProducerChannel";
    	return new $producerClass($this);
    }
    
    /**
     * @return Kafka_IConsumer
     */
    public function createConsumer()
    {
    	$consumerClass = "Kafka_{$this->kapiImplementation}_ConsumerChannel";
    	return new $consumerClass($this);
    }
}