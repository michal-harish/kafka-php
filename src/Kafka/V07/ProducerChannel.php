<?php

/**
 * Producer Channel
 *
 * Kafka 0.7 Producer Channel.
 *
 * @author     Michal Haris <michal.harish@gmail.com>
 * @date       2012-11-15
 */

namespace Kafka\V07;

use Kafka\Kafka;
use Kafka\Message;
use Kafka\IProducer;

require_once 'Channel.php';

class ProducerChannel
    extends Channel
    implements IProducer
{
    /**
     * Message queue
     *
     * @var Array
     */
    protected $messageQueue;

    /**
     * Constructor
     *
     * @param Kafka $connection
     */
    public function __construct(Kafka $connection)
    {
        parent::__construct($connection);
        $this->messageQueue = array();
    }

    /**
     * Add
     *
     * Add a single message to the produce queue.
     *
     * @param Message|string $message
     *
     * @return boolean Success
     */
    public function add(Message $message)
    {
        $this->messageQueue
            [$message->topic()]
            [$message->partition()]
            [] = $message;
    }

    /**
     * Produce
     *
     * Produce all messages added.
     *
     * @throws \Kafka\Exception On Failure
     *
     * @return true On Success
     */
    public function produce()
    {
        // in 0.7 kafka api we can only produce a message set to a single
        // topic-partition only so we'll do each individually.
        foreach ($this->messageQueue as $topic => &$partitions) {
            foreach ($partitions as $partition => &$messageSet) {
                $batchMessageData = null;
                $batchCompression = \Kafka\Kafka::COMPRESSION_NONE;
                $produceData = '';
                foreach ($messageSet as $message) {
                    if ($batchCompression != $message->compression()) {
                        if ($batchCompression != \Kafka\Kafka::COMPRESSION_NONE) {
                            $produceData .= $this->batchWrap(
                                $topic,
                                $partition,
                                $batchMessageData,
                                $batchCompression
                            );
                            $batchMessageData = null;
                        }
                        $batchCompression = $message->compression();
                    }

                    if ($batchCompression != \Kafka\Kafka::COMPRESSION_NONE) {
                        $batchMessageData .= $this->packMessage(
                            $message,
                            \Kafka\Kafka::COMPRESSION_NONE
                        );
                    } else {
                        $produceData .= $this->packMessage($message);
                    }
                }

                if ($batchMessageData !== null) {
                    $produceData .= $this->batchWrap(
                        $topic,
                        $partition,
                        $batchMessageData,
                        $batchCompression
                    );
                }

                // 0.7 api header
                $data = pack('n', \Kafka\Kafka::REQUEST_KEY_PRODUCE); //short
                $data .= pack('n', strlen($topic)) . $topic; //short string
                $data .= pack('N', $partition); //int

                // 0.7 produce messages
                $data .= pack('N', strlen($produceData));
                $data .= $produceData; //

                // in 0.7 kafka api there is no acknowledgement so
                // expectResponse for send is false
                if ($this->send($data, false)) {
                    // and therefore as long as the send() is happy we deem the
                    // message set to have been produced - unset the
                    // succesfully sent messageSet
                    unset($partitions[$partition]);
                }
                unset($messageSet);
            }
            unset($partitions);
        }
        // in 0.7 kafka api we skip any reading from the socket for now
        // because no acknowledgements come from Kafka and we return the
        // success only.
        return true;
    }

    /**
     * Batch wrap
     *
     * Internal for producing batch of consecutive messages with the same
     * compress codec.
     *
     * @returns A compressed payload representing a batch of messages
     */
    private function batchWrap(
        $topic,
        $partition,
        &$batchMessageData,
        $batchCompression)
    {
        if ($batchCompression != \Kafka\Kafka::COMPRESSION_NONE) {
            $batchPayload = $this->packMessage(
                new Message(
                    $topic,
                    $partition,
                    $batchMessageData,
                    $batchCompression
                )
            );

            return $batchPayload;
        }
    }
}
