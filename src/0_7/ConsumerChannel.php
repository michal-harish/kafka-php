<?php
/**
 * Consumer channel for FetchRequest and OffsetReqeust.
 * 
 * @author michal.harish@gmail.com
 *
 */

include_once 'Channel.php';

class Kafka_0_7_ConsumerChannel extends Kafka_0_7_Channel
implements Kafka_IConsumer
{

    /**
     * @var string
     */
    private $topic;
    
    /**
     * @var int
     */
    private $partition;

    /**
     * @var Kafka_Offset
     */
    private $offset;

    /**
     * @param Kafka $connection 
     */
    public function __construct(Kafka $connection)
    {
        parent::__construct($connection);
    }

    /**
     * @param string $topic
     * @param int $partition 
     * @param Kafka_Offset $offset - Offset to fetch messages from
     * @param int $maxFetchSize - Maximum bytes in a single fetch request 
     * @throws Kafka_Exception
     * @return bool Ready-to-read state
     */
    public function fetch( 
        $topic,
        $partition = 0,
        Kafka_Offset $offset = NULL, 
        $maxFetchSize = 1000000
    )
    {
        if (!$topic || !is_string($topic))
        {
            throw new Kafka_Exception(
                   "Topic must be a non-empty string."
            );
        }
        $this->topic = $topic;
        $this->partition = $partition;

        if ($offset === NULL)
        {
            $this->offset = new Kafka_Offset();
        }
        else
        {
            $this->offset = clone $offset;
        }
        if (!is_numeric($maxFetchSize) || $maxFetchSize <=0)
        {
            throw new Kafka_Exception(
                "Maximum fetch size must be a positive integer."
            );
        }
        //format the 0.7 fetch request 
        $data = pack('n', Kafka::REQUEST_KEY_FETCH);//short
        $data .= pack('n', strlen($this->topic)) . $this->topic;//short string
        $data .= pack('N', $this->partition);//int
        $data .= $this->offset->getData();//bigint
        $data .= pack('N', $maxFetchSize); //int
        if ($this->send($data))
        {
            return $this->hasIncomingData();
        }
    }

    /**
     * The main method that pulls for messages
     * to come downstream. If there are no more messages
     * it will turn off the readable state and return FALSE
     * so that next time another request is made to the connection
     * if the program wants to check again later.
     *
     * @throws Kafka_Exception
     * @return Kafka_Message | FLASE if no more messages
     */
    public function nextMessage()
    {
        try {
            if ($this->hasIncomingData())
            {
                $message = $this->loadMessage(
                    $this->topic,
                    $this->partition,
                    clone $this->offset
                );
                $this->offset->addInt($this->getReadBytes());

                return $message;
            }
            else
            {
                return FALSE;
            }
        } catch (Kafka_Exception_EndOfStream $e) {
            return FALSE;
        }
    }

    /**
     * The last offset position after connection or reading nextMessage().
     * This value should be used for keeping the consumption state.
     * It is different from the nextMessage()->getOffset() in that
     * it points to the offset "after" that message.
     *
     * @return Kafka_Offset
     */
    public function getWatermark()
    {
        return clone $this->offset;
    }
    
    /**
     * OffsetsRequest
     * Enter description here ...
     * @param string $topic
     * @param int $partition
     * @param mixed $time 
     * @param unknown_type $maxNumOffsets
     */
    public function offsets(
        $topic,
        $partition = 0,
        $time = Kafka::OFFSETS_LATEST,
        $maxNumOffsets = 2
    )
    {
        $data = pack('n', Kafka::REQUEST_KEY_OFFSETS);
        $data .= pack('n', strlen($topic)) . $topic;
        $data .= pack('N', $partition);
        if (is_string($time))
        {
            //convert hex constant to a long offset
            $offset = new Kafka_Offset($time);
        }
        else
        {    //make 64-bit unix timestamp offset
            $offset = new Kafka_Offset();
            for($i=0; $i<1000 * 1; $i++)
            {
                $offset->addInt($time);
            }
        }
        $data .= $offset->getData();
        $data .= pack('N', $maxNumOffsets);
        $this->send($data);
        if ($this->hasIncomingData())
        {
            $offsetsLength = array_shift(unpack('N', $this->read(4)));
            if ($offsetsLength>0)
            {
                $offsets = array_fill(0, $offsetsLength, NULL);
                for($i=0; $i<$offsetsLength; $i++)
                {
                    $offset = Kafka_Offset::createFromData($this->read(8));
                    $offsets[$i] = $offset;
                }
                if (!$this->hasIncomingData())
                {
                    return $offsets;
                }
            }
        }
       return FALSE;
    }
}