<?php 
class Kafka_ProduceRequest
{
    //connection properties
    private $host;
    private $port;
    private $timeout;
    //connection state internals
    private $connection = NULL;

    //publish internals
    private $topic;
    private $partition;

    /**
     * @param string $topic - topic name to publish to
     * @param int $partition - broker partition
     * @param string $host - Kafka broker host
     * @param int $port - Kafka broker port
     * @param int $timeout - Kafka broker connection timeout
     */
    public function __construct(
        $topic,
        $partition = 0,
        $host = 'localhost',
        $port = 9092,
        $timeout = 5
    )
    {
        $this->topic = $topic;
        $this->partition = 0;
        $this->host = $host;
        $this->port = $port;
        $this->timeout = $timeout;
    }

    /**
     * @param array $messages
     * @param int $compression
     */
    public function publish(Kafka_Message $message)
    {
        $messageSet = array($message);
        return $this->produce($messageSet);
    }

    /**
     * @param array $messageSet
     */
    public function produce(array $messageSet)
    {
        $messageSetSize = 0;
        foreach($messageSet as $message)
        {
            $messageSetSize += $message->size();
        }
        $this->connect();
        fwrite($this->connection, pack('N', 2 + 2 + strlen($this->topic) + 4 + 4 + $messageSetSize)); //
        fwrite($this->connection, pack('n', Kafka_Broker::REQUEST_KEY_PRODUCE));
        fwrite($this->connection, pack('n', strlen($this->topic)));
        fwrite($this->connection, $this->topic);
        fwrite($this->connection, pack('N', $this->partition));
        fwrite($this->connection, pack('N', $messageSetSize)); //
        foreach($messageSet as $message)
        {
            $message->writeTo($this->connection);
        }
    }

    /**
      * Close the connection. Must be called by the application 
     * but could be added to the __destruct method too.
     */
    public function close() {
        if (is_resource($this->connection)) {
            fclose($this->connection);
        }
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