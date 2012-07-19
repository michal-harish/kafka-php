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
include "Request.php";
include "OffsetRequest.php";
include "FetchRequest.php";
include "ProduceRequest.php";

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
    
    /**
     * Connection socket.
     * @var resource
     */
    private $connection = NULL;
    
    /**
     * Socket ready state for another request.  
     * @var bool
     */
    private $ready;
    
    /**
     * @param string $host
     * @param int $port
     * @param int $timeout
     */
    public function __construct(
	    $host = 'localhost',
	    $port = 9092,
	    $timeout = 5    
    )
    {
    	$this->host = $host;
    	$this->port = $port;
    	$this->timeout = $timeout;
    }
    
    /**
     * Set up the socket connection if not yet done.
     * @throws Kafka_Exception
     * @return resource $connection
     */
    public function getSocket()
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
    		$this->ready = TRUE;
    	}
    	if (!$this->ready){
    		return FALSE;
    	}
    	return $this->connection;
    }
    
    /**
     * Close the connection(s). Must be called by the application
     * but could be added to the __destruct method too.
     */
    public function close() {
    	if (is_resource($this->connection)) {
    		fclose($this->connection);
    	}
    }
}