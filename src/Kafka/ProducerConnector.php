<?php

/**
 * Kafka Producer Connector
 *
 * Producer Connector is the class that will provide the main methods to
 * spray Kafka Messages to all the active broker/partitions.
 *
 * @author     Pau Gay <pau.gay@gmail.com>
 * @date       2012-11-15
 */

namespace Kafka;

class ProducerConnector
{
    /**
     * Zookeeper Connection
     *
     * @var Zookeeper
     */
    private $zk;

    /**
     * Topic to broker mapping
     *
     * Mapping that will contain which topics are using which
     * brokers/partitions.
     *
     * @format
     *
     *     array(
     *         "{topic-1}" => array(
     *             0 => array(
     *                 "broker"    => {broker},
     *                 "partition" => {partition}
     *             ),
     *             ...
     *         ),
     *         ...
     *     )
     *
     * @var Array
     */
    private $topicPartitionMapping;

    /**
     * Producer list
     *
     * List of Kafka Producer Channels that provide the connection to the
     * different partitions.
     *
     * @var Array of IProducer
     */
    private $producerList;

    /**
     * Construct
     */
    public function __construct($zkConnect)
    {
        $this->zk = new \Zookeeper($zkConnect);

        $this->discoverTopics();
    }

    /**
     * Discover topics
     *
     * Method that will discover the Kafka topics stored in Zookeeper.
     * The method will populate the topicPartitionMapping array.
     */
    private function discoverTopics()
    {
        // get the list of topics
        $topics = $this->zk->getChildren("/brokers/topics");

        foreach ($topics as $topic) {
            // get the list of brokers by topic
            $topicBrokers = $this->zk->getChildren("/brokers/topics/$topic");
            foreach ($topicBrokers as $brokerId) {
                // get the number of partitions
                $partitionCount = $this->zk->get(
                    "/brokers/topics/$topic/$brokerId"
                );
                for ($p = 0; $p < $partitionCount; $p++) {
                    // add it to the mapping
                    $this->topicPartitionMapping[$topic][] = array(
                        "broker"    => $brokerId,
                        "partition" => $p,
                    );
                }
            }
        }
    }

    /**
     * Add message
     *
     * Method that will build a message given the payload, will randomly
     * decide which is the partition where we are going to send the
     * message and will send it.
     *
     * @param String $topic
     * @param String $payload
     * @param Integer $compression
     */
    public function addMessage(
        $topic,
        $payload,
        $compression = \Kafka\Kafka::COMPRESSION_NONE
    )
    {
        // random paritioner hardcode for now
        // TODO create Partitioner class and \Kafka\Partitioner

        // randomly get which partition we will use
        $i = rand(0, count($this->topicPartitionMapping[$topic]) - 1);

        // get partition information
        $partitionInfo = $this->topicPartitionMapping[$topic][$i];

        $brokerId  = $partitionInfo['broker'];
        $partition = $partitionInfo['partition'];

        // build the message
        $message = new Message(
            $topic,
            $partition,
            $payload,
            $compression
        );

        // get the actual producer we will add the mesasge
        $producer = $this->getProducerByBrokerId($brokerId);

        $producer->add($message);
    }

    /**
     * Produce
     *
     * This method will actually produce the reall messages to Kafka.
     */
    public function produce()
    {
        foreach ($this->producerList as $producer) {
            $producer->produce();
        }
    }

    /**
     * Get producer by broker id
     *
     * Method that given a broker id, it will create the producer and
     * will return it.
     *
     * @return IProducer
     */
    private function getProducerByBrokerId($brokerId)
    {
        if (!isset($this->producerList[$brokerId])) {
            $brokerInfo = $this->zk->get("/brokers/ids/$brokerId");
            $parts = explode(":", $brokerInfo);

            // loose the first part
            array_shift($parts);
            list($host, $port) = $parts;

            // instantiate the kafka broker representation
            $kafka = new Kafka($host, $port);
            $this->producerList[$brokerId] = $kafka->createProducer();
        }

        return $this->producerList[$brokerId];
    }
}
