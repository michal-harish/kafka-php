<?php

/**
 * Kafka Message Stream
 *
 * Consumer Connector is the class that will provide the main methods to
 * access to Kafka messages.
 *
 * @author     Pau Gay <pau.gay@gmail.com>
 */

class Kafka_MessageStream
{
    /**
     * Consumer
     *
     * The consumer that will provide the actual consumer from where we
     * will fetch and get the message.
     *
     * @var Kafka_IConsumer
     */
    private $consumer;

    /**
     * Topic
     *
     * Name of the topic that we want to consume.
     *
     * @var String
     */
    private $topic;

    /**
     * Partition
     *
     * Identifier of the partition that we want to look at.
     *
     * @var Integer
     */
    private $partition;

    /**
     * Max fetch size
     *
     * Number of bytes (in Integer) that we want to fetch.
     *
     * @var Integer
     */
    private $maxFetchSize;

    /**
     * Offset
     *
     * @var Kafka_Offset
     */
    private $offset;

    /**
     * Has fetched
     *
     * Boolean that will remind us if we have fetched or not before.
     *
     * @var Boolean
     */
    private $hasFetched = false;


    /**
     * Construct
     *
     * @param Kafka $kafka
     * @param String $topic
     * @param String $partition
     * @param Integer $maxFetchSize
     */
    public function __construct(
        Kafka $kafka,
        $topic,
        $partition,
        $maxFetchSize
    )
    {
        $this->consumer     = $kafka->createConsumer();
        $this->topic        = $topic;
        $this->partition    = $partition;
        $this->maxFetchSize = $maxFetchSize;
        $this->offset       = $this->getSmallestOffset();
    }

    /**
     * Next message
     *
     * Method that will fetch (if we need to, according to $hasFetch) and return
     * the next message.
     *
     * @return Boolean|Message
     */
    public function nextMessage()
    {
        if (!$this->hasFetched) {
            $this->hasFetched = $this->consumer->fetch(
                $this->topic,
                $this->partition,
                $this->offset,
                $this->maxFetchSize
            );

            if (!$this->hasFetched) {
                return false;
            }
        }

        $message =  $this->consumer->nextMessage();

        if (!$message) {
            $this->hasFetched = false;
        } else {
            $this->offset = $this->consumer->getWatermark();
        }

        return $message;
    }

    /**
     * Get smallest offset
     *
     * Method that will return the smallest offset (it's rarely "0", so
     * want to look it up in order to not fail).
     *
     * @return Kafka_Offset
     */
    private function getSmallestOffset()
    {
        $offsets = $this->consumer->offsets(
            $this->topic,
            $this->partition,
            Kafka::OFFSETS_EARLIEST
        );

        return new Kafka_Offset($offsets[0]);
    }
}
