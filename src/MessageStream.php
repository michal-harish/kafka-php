<?php

class MessageStream
{
    private $consumer;

    private $offset;

    // remember that we haven't fetched anything
    private $hasFetched = FALSE;

    /**
     * TODO
     */
    public function __construct(
        Kafka $kafka,
        $topic,
        $partition,
        $maxFetchSize
    )
    {
        $this->consumer = $kafka->createConsumer();
        $this->topic = $topic;
        $this->partition = $partition;
        $this->maxFetchSize = $maxFetchSize;

        $this->offset = $this->getSmallestOffset();
    }

    public function nextMessage()
    {

        if (!$this->hasFetched)
        {

            if (!$this->hasFetched = $this->consumer->fetch(
                $this->topic,
                $this->partition,
                $this->offset,
                $this->maxFetchSize
            ))
            {
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

    public function getSmallestOffset()
    {
        $offsets = $this->consumer->offsets(
            $this->topic,
            $this->partition,
            Kafka::OFFSETS_EARLIEST
        );

        return new Kafka_Offset($offsets[0]);
    }
}
