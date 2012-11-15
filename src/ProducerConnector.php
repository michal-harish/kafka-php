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

class Kafka_ProducerConnector
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
     * Mapping that will contain which topics are using which brokers.
     *
     * @format
     *
     *     array(
     *         "{topic-1}" => array(
     *             0 => {brokerId},{partition},
     *             n => {brokerId},
     *             ...
     *         ),
     *         "{topic-2}" => array(
     *             "{broker-1}" => {brokerId},
     *             "{broker-2}" => {brokerId},
     *             ...
     *         )
     *     )
     *
     * @var Array
     */
    private $topicPartitionMapping;

    /**
     * Kafka Producer Channel list
     *
     * List of Kafka producers that will provide the connection to the
     * different partitions, keyed by brokerId.
     *
     * @var Array
     */
    private $producerList;

    /**
     * Construct
     */
    public function __construct($zkConnect)
    {
        $this->zk = new Zookeeper($zkConnect);

        $this->discoverTopics();

    }

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

                for ($partition = 0; $partition < $partitionCount; $partition++) {
                    // add it to the mapping
                    $this->topicPartitionMapping[$topic][] = array(
                        "broker"    => $brokerId,
                        "partition" => $partition,
                    );
                }
            }
        }
        //print_r($this->topicPartitionMapping);
    }

    public function addMessage(
        $topic,
        $payload,
        $compression = Kafka::COMPRESSION_NONE
    )
    {
        // figure out the partition
        $i = rand(0, count($this->topicPartitionMapping[$topic]) - 1);
        $partitionInfo = $this->topicPartitionMapping[$topic][$i];
        $brokerId = $partitionInfo['broker'];
        $partition = $partitionInfo['partition'];
        $producer = $this->getProducerByBrokerId($brokerId);

        $message = new Kafka_Message(
            $topic,
            $partition,
            $payload,
            $compression
        );

        $producer->add($message);
    }

    public function produce()
    {
        foreach($this->producerList as $producer)
        {
            $producer->produce();
        }
    }

    private function getProducerByBrokerId($brokerId)
    {
        if (!isset($this->producerList[$brokerId]))
        {

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
