<?php
/**
 * Kafka 0.7 Producer Channel.
 *
 * In this version there is no acknowledgement that the message has
 * been received by the broker so the success is measured only
 * by writing succesfully into the socket.
 *
 * The channel implementation, however is the same as in 0.7
 *
 * @author michal.harish@gmail.com
 */

namespace Kafka\V08;

use Kafka\Kafka;
use Kafka\Message;

require_once realpath(dirname(__FILE__) . '/../V07/Channel.php');

class ProducerChannel
    extends Channel
    implements IProducer
{
    /**
     * @var array
     */
    private $messageQueue;

    /**
     * @param Kafka $connection
     */
    public function __construct(Kafka $connection)
    {
        parent::__construct($connection);
        $this->messageQueue = array();
    }

    /**
     * Add a single message to the produce queue.
     *
     * @param  Message|string $message
     * @return boolean        Success
     */
    public function add(Message $message)
    {
        $this->messageQueue
            [$message->topic()]
            [$message->partition()]
            [] = $message;
    }

    /**
     * Produce all messages added.
     * @throws \Kafka\Exception On Failure
     * @return TRUE             On Success
     */
    public function produce()
    {

        //in 0.8, a new wire format was introduced which contains request versions
        $versionId = 0;
        $correlationId = 0;
        $clientId = '';
        $requiredAcks = 1;
        $numTopics = 1;

        //produce request header
        $data = pack('n', \Kafka\Kafka::REQUEST_KEY_PRODUCE); //short
        $data .= pack('n', $versionId);//short
        $data .= pack('N', $correlationId);//int
        $data .= pack('n', strlen($clientId)) . $clientId;//short string
        $data .= pack('n', $requiredAcks);//short
        $data .= pack('N', $ackTimeoutMs = 5000);//int
        //produce request topic structure
        $numTopics = count($this->messageQueue);
        $data .= pack('N', $numTopics);//int
        foreach ($this->messageQueue as $topic => $partitions) {
            $data .= pack('n', strlen($topic)) . $topic;//short string
            $data .= pack('N', count($partitions));//int
            foreach ($partitions as $partition => $messageSet) {
                $data .= pack('N', $partition);//int
                $messageSetData = '';
                foreach ($messageSet as $message) {
                    $messageSetData .= $this->packMessage($message);
                }
                $data .= pack('N', strlen($messageSetData)); //int
                $data .= $messageSetData; //
            }
        }
        if ($this->send($data)) {
               if ($this->hasIncomingData()) {
                   $this->messageQueue = array();
                   if ($this->getRemainingBytes() == 0) {
                       throw new \Kafka\Exception(
                           "Something went wrong but Kafka is not sending propper error code"
                       );
                   }
                   exit("!" . $this->getRemainingBytes() . "\n");

                   return TRUE;

               } else {
                    throw new \Kafka\Exception("Produce request was not acknowledged by the broker");
               }
        }
        throw new \Kafka\Exception("Produce request was not sent.");
    }
}
