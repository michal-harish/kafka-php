<?php 
/**
 * ProduceRequest is a connection to a topic which implements
 * multiple ways of publishing single message or set of messages.
 * 
 * Currently there is no acknowledgement that the message has 
 * been received by the broker.
 * 
 * @author michal.harish@gmail.com
 */

class Kafka_ProduceRequest extends Kafka_Request
{
    /**
     * Default compression type for messages passed as simple strings
     * @var boolean
     */
    private $defaultCompression;

    /**
     * @var array
     */
    private $messageQueue;

    /**
     * @param Kafka $connection
     * @param string $topic
     * @param int $partition 
     * @param int $defaultCompression Compression type for messages passed as simple strings 
     */
    public function __construct(
        Kafka $connection,
        $topic,
        $partition = 0,
        $defaultCompression = Kafka::COMPRESSION_GZIP
    )
    {
        parent::__construct($connection, $topic, $partition);
        $this->defaultCompression = $defaultCompression;
        $this->messageQueue = array();
    }

    /**
     * Internal fired directly from the hasIncomingData to initiate conversation with Kafka broker
     * in this case sending a fetch request.
     * @throws Kafka_Exception
     * @return bool Ready-to-read state
     */
    protected function send()
    {
        if (!$this->isWritable())
        {
            throw new Kafka_Exception(
                "Kafka Request channel is not writable, there is data to be read from previous response."
            );
        }
        $messageSetSize = 0;
        foreach($this->messageQueue as $message)
        {
            $messageSetSize += $message->size();
        }
        $requestSize = 2 + 2 + strlen($this->topic) + 4 + 4 + $messageSetSize;
        $socket = $this->connection->getSocket();
        $written = fwrite($socket, pack('N', $requestSize));
        $written += fwrite($socket, pack('n', Kafka::REQUEST_KEY_PRODUCE));
        $written += fwrite($socket, pack('n', strlen($this->topic)));
        $written += fwrite($socket, $this->topic);
        $written += fwrite($socket, pack('N', $this->partition));
        $written += fwrite($socket, pack('N', $messageSetSize)); //
        foreach($this->messageQueue as $message)
        {
            $written += $message->writeTo($socket);
        }
        if ($written  != $requestSize + 4)
        {
            throw new Kafka_Exception(
                "ProduceRequest written $written bytes, expected to send:" . ($requestSize + 4)
            );
        }
        //Ready-to-read state is TRUE once acknowledgements are in place
        return TRUE;
    }

    /**
     * Publish a single message to the topic of this request.
     *
     * @param Kafka_Message|string $message
     * @return boolean Success
     */
    public function add($message)
    {
        if (is_string($message))
        {
            //it's a payload string instead of object
            $message = Kafka_Message::create(
                $message,
                $this->defaultCompression
            );
        }
        if (!$message instanceof Kafka_Message)
        {
            throw new Kafka_Exception(
                "Message must be a Kafka_Message object or string."
            );
        }
        $this->messageQueue[] = $message;
    }

    /**
     * Produce a set of messages to the topic of this request.
     * @param Kafka_Message|string $message Optional message for single-message produce requests
     * @return boolean Success
     */
    public function produce($message = NULL)
    {
        if ($message !== NULL)
        {
            $this->add($message);
        }
        //we skip any reading from the socket for now
        //because no acknowledgements come from Kafka
        //and we return the success only.
        if ($success = $this->send())
        {
            $this->messageQueue = array();
        }
        return $success; 
    }
}