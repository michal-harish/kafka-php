<?php

/**
 * Kafka Consumer Connector
 *
 * Consumer Connector is the class that will provide the main methods to
 * access to Kafka messages.
 *
 * @author     Pau Gay <pau.gay@gmail.com>
 */

namespace Kafka;

class ConsumerConnector
{
    /**
     * Zookeeper Connection
     *
     * @var Zookeeper
     */
    private $zk;

    /**
     * Kafka Broker list
     *
     * List of Kafka brokers that will provide the connection to the
     * different partitions.
     *
     * @var Array
     */
    private $kafkaBrokerList;

    /**
     * Construct
     */
    public function __construct($zkConnect)
    {
        $this->zk = new \Zookeeper($zkConnect);
    }

    /**
     * Create message streams
     *
     * @param String $topic
     * @param Integer $maxFetchSize
     *
     * @return Array Array containing the list of consumer streams
     */
    public function createMessageStreams($topic, $maxFetchSize = 1000)
    {
        $topicBrokers = $this->zk->getChildren("/brokers/topics/$topic");

        $messageStreams = array();

        foreach ($topicBrokers as $brokerId) {
            $partitionCount = $this->zk->get(
                "/brokers/topics/$topic/$brokerId"
            );

            $kafka = $this->getKafkaByBrokerId($brokerId);

            for ($partition = 0; $partition < $partitionCount; $partition++) {
                $messageStreams[] = new MessageStream(
                    $kafka,
                    $topic,
                    $partition,
                    $maxFetchSize
                );
            }
        }

        return $messageStreams;
    }

    /**
     * Get Kafka by Broker Id.
     *
     * @param String $brokerId
     *
     * @return Kafka
     */
    private function getKafkaByBrokerId($brokerId)
    {
        // check if it exists, and if it doesn't, create it
        if (!isset($this->brokerList[$brokerId])) {
            // will return something like:
            // {something}-{numbers_that_looks_like_timestamp}:{host}:{port}
            $tmp = $this->zk->get("/brokers/ids/$brokerId");

            $parts = explode(":", $tmp);

            // loose the first part
            array_shift($parts);

            list($host, $port) = $parts;

            // instantiate the kafka broker representation
            $kafka = new Kafka($host, $port);

            // add the kafka bronker to the list
            $this->kafkaBrokerList[$brokerId] = $kafka;
        }

        return $this->kafkaBrokerList[$brokerId];
    }
}
