<?php

/**
 * Consumer Channel
 *
 * Consumer channel for FetchRequest and OffsetReqeust.
 *
 * @author     Michal Haris <michal.harish@gmail.com>
 * @date       2012-11-15
 */

namespace Kafka\V07;

use Kafka\Kafka;
use Kafka\Message;
use Kafka\Offset;
use Kafka\IConsumer;

require_once 'Channel.php';

class ConsumerChannel
    extends Channel
    implements IConsumer
{
    /**
     * Topic
     *
     * @var String
     */
    private $topic;

    /**
     * Partition
     *
     * @var Integer
     */
    private $partition;

    /**
     * Offset
     *
     * @var Offset
     */
    private $offset;

    /**
     * Constructor
     *
     * @param Kafka $connection
     */
    public function __construct(Kafka $connection)
    {
        parent::__construct($connection);
    }

    /**
     * Fetch
     *
     * @param string $topic
     * @param int    $partition
     * @param Offset $offset       - Offset to fetch messages from
     * @param int    $maxFetchSize - Maximum bytes in a single fetch request
     *
     * @throws \Kafka\Exception
     *
     * @return bool Ready-to-read state
     */
    public function fetch(
        $topic,
        $partition = 0,
        Offset $offset = null,
        $maxFetchSize = 1000000
    )
    {
        if (!$topic || !is_string($topic)) {
            throw new \Kafka\Exception(
                   "Topic must be a non-empty string."
            );
        }
        $this->topic = $topic;
        $this->partition = $partition;

        if ($offset === null) {
            $this->offset = new Offset();
        } else {
            $this->offset = clone $offset;
        }
        if (!is_numeric($maxFetchSize) || $maxFetchSize <=0) {
            throw new \Kafka\Exception(
                "Maximum fetch size must be a positive integer."
            );
        }
        //format the 0.7 fetch request
        $data = pack('n', \Kafka\Kafka::REQUEST_KEY_FETCH);//short
        $data .= pack('n', strlen($this->topic)) . $this->topic;//short string
        $data .= pack('N', $this->partition);//int
        $data .= $this->offset->getData();//bigint
        $data .= pack('N', $maxFetchSize); //int
        if ($this->send($data)) {
            return $this->hasIncomingData();
        }
    }

    /**
     * Next message
     *
     * The main method that pulls for messages
     * to come downstream. If there are no more messages
     * it will turn off the readable state and return false
     * so that next time another request is made to the connection
     * if the program wants to check again later.
     *
     * @throws \Kafka\Exception
     *
     * @return \Kafka\Message | false if no more messages
     */
    public function nextMessage()
    {
        try {
            if ($this->hasIncomingData()) {
                $message = $this->loadMessage(
                    $this->topic,
                    $this->partition,
                    clone $this->offset
                );
                $this->offset->addInt($this->getReadBytes());

                return $message;
            } else {
                return false;
            }
        } catch (\Kafka\Exception\EndOfStream $e) {
            return false;
        }
    }

    /**
     * Get watermark
     *
     * The last offset position after connection or reading nextMessage().
     * This value should be used for keeping the consumption state.
     * It is different from the nextMessage()->getOffset() in that
     * it points to the offset "after" that message.
     *
     * @return Offset
     */
    public function getWatermark()
    {
        return clone $this->offset;
    }

    /**
     * Offsets
     *
     * @param string       $topic
     * @param int          $partition
     * @param mixed        $time
     * @param unknown_type $maxNumOffsets
     */
    public function offsets(
        $topic,
        $partition = 0,
        $time = \Kafka\Kafka::OFFSETS_LATEST,
        $maxNumOffsets = 2
    )
    {
        $data = pack('n', \Kafka\Kafka::REQUEST_KEY_OFFSETS);
        $data .= pack('n', strlen($topic)) . $topic;
        $data .= pack('N', $partition);

        if (is_string($time) || $time <0) {
            //convert literal to Offset
            $offset = new Offset($time);
        } else {
            //make 64-bit unix timestamp offset
            $offset = new Offset();
            for ($i=0; $i<1000 * 1; $i++) {
                $offset->addInt($time);
            }
        }
        $data .= $offset->getData();
        $data .= pack('N', $maxNumOffsets);
        $this->send($data);

        if ($this->hasIncomingData()) {
            $h = unpack('N', $this->read(4));
            $offsetsLength = array_shift($h);
            if ($offsetsLength>0) {
                $offsets = array_fill(0, $offsetsLength, null);
                for ($i=0; $i<$offsetsLength; $i++) {
                    $offset = new Offset();
                    $offset->setData($this->read(8));
                    $offsets[$i] = $offset;
                }
                if (!$this->hasIncomingData()) {
                    return $offsets;
                }
            }
        }

        return false;
    }
}
