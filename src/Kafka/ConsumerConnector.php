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

final class ConsumerConnector
{
    /**
     * Metadata
     *
     * @var \Kafka\IMetadata
     */
    private $metadata;

    /**
     * Kafka Broker list
     *
     * List of Kafka brokers that will provide the connection to the
     * different partitions.
     *
     * @var Array
     */
    private $brokerList;

    /**
     * Topic Metadata
     *
     * @var array[<topic>][<virtual_partition>]
     */
    private $topicMetadata = array();

    /**
     * Create
     *
     * @param String $connectionString
     * @param Float  $apiVersion
     */
    public static function Create(
        $connectionString,
        $apiVersion = 0.7
    )
    {
        $apiImplementation = Kafka::getApiImplementation($apiVersion);
        include_once "{$apiImplementation}/Metadata.php";
        $metadataClass = "\\Kafka\\{$apiImplementation}\\Metadata";
        $connector = new ConsumerConnector(
            new $metadataClass($connectionString)
        );

        return $connector;
    }

    /**
     * Constructor
     *
     * @param IMetadata $metadata
     */
    protected function __construct(IMetadata $metadata)
    {
        $this->metadata = $metadata;
        $this->topicMetadata = $this->metadata->getTopicMetadata();
    }

    /**
     * Create message streams by filter
     *
     * Create message streams by a given TopicFilter (either Whitelist or
     * Blacklist) with a given fetch size applied to each.
     *
     * @param  TopicFilter $filter
     * @param  Integer     $maxFetchSize
     * @param  Integer     $offset
     * @return Array       Array containing the list of consumer streams
     */
    public function createMessageStreamsByFilter(
        TopicFilter $filter,
        $maxFetchSize = 1000,
        $offset = \Kafka\Kafka::OFFSETS_LATEST
    )
    {
        $messageStreams = array();
        $topics = $filter->getTopics(array_keys($this->topicMetadata));
        foreach ($topics as $topic) {
            $topicMessageStreams = $this->createMessageStreams(
                $topic,
                $maxFetchSize,
                $offset
            );
            foreach ($topicMessageStreams as $messageStream) {
                $messageStreams[] = $messageStream;
            }
        }

        return $messageStreams;
    }

    /**
     * Create message streams
     *
     * @param  String  $topic
     * @param  Integer $maxFetchSize
     * @param  Integer $offset
     * @return Array   Array containing the list of consumer
     *                 streams
     */
    public function createMessageStreams(
        $topic,
        $maxFetchSize = 1000,
        $offset = \Kafka\Kafka::OFFSETS_LATEST
    )
    {
        if (!isset($this->topicMetadata[$topic])) {
            throw new \Kafka\Exception("Unknown topic `{$topic}`");
        }
        $messageStreams = array();
        foreach ($this->topicMetadata[$topic] as $virtualPartition) {
            $broker = $this->getKafkaByBrokerId($virtualPartition['broker']);
            $partition = $virtualPartition['partition'];
            $messageStreams[] = new MessageStream(
                $broker,
                $topic,
                $partition,
                $maxFetchSize,
                $offset
            );
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
            $broker = $this->metadata->getBrokerInfo($brokerId);

            // instantiate the kafka broker representation
            $kafka = new Kafka($broker['host'], $broker['port']);

            // add the kafka bronker to the list
            $this->brokerList[$brokerId] = $kafka;
        }

        return $this->brokerList[$brokerId];
    }
}
