<?php
/**
 * Generic fetch request channel.
 * 
 * @author michal.harish@gmail.com
 *
 */

abstract class Kafka_Request
{
    /**
     * Connection object 
     * @var Kafka
     */
    protected $connection;

    /**
     * @var string
     */
    protected $topic;

    /**
     * @var int
     */
    protected $partition;

    /**
     * Response of a readable channel
     * @var int
     */
    protected $responseSize;

    /**
     * Request channel state
     * @var boolean
     */
    private $readable;


    /**
     * @param Kafka $connection 
     * @param string $topic
     * @param int $partition 
     */
    public function __construct(
        Kafka $connection,
        $topic,
        $partition = 0
    )
    {
        $this->connection = $connection;
        if (!$topic)
        {
            throw new Kafka_Exception("Topic name cannot be an empty string.");
        }
        $this->topic = $topic;
        if (!is_numeric($partition) || $partition < 0)
        {
            throw new Kafka_Exception("Partition must be a positive integer or 0.");
        }
        $this->partition = $partition;
        $this->readable = FALSE;
    }

    /**
     * Internal fired from the hasIncomingData
     * to initiate the conversation with Kafka broker.
     * @throws Kafka_Exception
     * @return bool Ready to read state
     */
    abstract protected function send();

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
     * Every response handler has to call this method
     * to validate state of the channel and read
     * standard kafka channel headers.
     * @throws Kafka_Exception
     * @return boolean
     */
    protected function hasIncomingData()
    {
        //check the state of the connection
        if (!$this->readable)
        {
            $this->responseSize = NULL;
            if (!$this->readable = $this->send())
            {
                throw new Kafka_Exception(
                    "Request has not been sent - maybe a connection problem."
                );
            }
        }
        $socket = $this->connection->getSocket();
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
            $this->readable = FALSE;
            $this->responseSize = NULL;
            return FALSE;
        } else 
        {
            return TRUE;
        }
    }

}