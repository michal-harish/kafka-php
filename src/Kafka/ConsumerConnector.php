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
     * Group this consumer belongs to
     * @var Sting
     */
    private $groupId;

    /**
     * Process within the consumer group
     * @var String
     */
    private $processId;

    /**
     * Create
     *
     * @param String $connectionString
     * @param Float  $apiVersion
     */
    public static function Create(
        $connectionString,
        $groupId,
        $apiVersion = 0.7
    )
    {
        $apiImplementation = Kafka::getApiImplementation($apiVersion);
        include_once "{$apiImplementation}/Metadata.php";
        $metadataClass = "\\Kafka\\{$apiImplementation}\\Metadata";
        $connector = new ConsumerConnector(
            new $metadataClass($connectionString),
            $groupId
        );

        return $connector;
    }

    /**
     * Constructor
     *
     * @param IMetadata $metadata
     * @param String $groupId
     */
    protected function __construct(IMetadata $metadata, $groupId)
    {
        $this->metadata = $metadata;
        $this->topicMetadata = $this->metadata->getTopicMetadata();
        $this->groupId = $groupId;
        $this->processId = gethostname() . "-" . uniqid();
        $this->metadata->registerConsumerProcess($this->groupId, $this->processId);
    }

    /**
     * Create message streams
     *
     * @param  String  $topic
     * @param  Integer $maxFetchSize
     * @param  Integer $offsetReset
     * @return Array   Array containing the list of consumer
     *                 streams
     */
    public function createMessageStreams(
        $topic,
        $maxFetchSize = 1000,
        $offsetReset = \Kafka\Kafka::OFFSETS_LATEST
    )
    {
        return $this->createMessageStreamsByFilter(new Whitelist("$topic"), $maxFetchSize, $offsetReset);
    }

    /**
     * Create message streams by filter
     *
     * Create message streams by a given TopicFilter (either Whitelist or
     * Blacklist) with a given fetch size applied to each.
     *
     * @param  TopicFilter $filter
     * @param  Integer     $maxFetchSize
     * @param  Integer     $offsetReset
     * @return ConsumerContext  Iterable boject of actual streams
     */
    public function createMessageStreamsByFilter(
        TopicFilter $filter,
        $maxFetchSize = 1000,
        $offsetReset = \Kafka\Kafka::OFFSETS_LATEST
    )
    {
        $streams = array(); 
        //TODO keep filter in a field and watch for new/removed topics
        $topics = $filter->getTopics(array_keys($this->topicMetadata));

        $context = new ConsumerContext();
        $this->startFetching($context, $topics, $maxFetchSize, $offsetReset);
        return $context;

    }

    private function startFetching(ConsumerContext $context, array $topics, $maxFetchSize, $offsetReset) {

        $context->closeAllStreams();
        $streams = array();
        foreach ($topics as $topic) {

            $partitions = $this->getOwnedPartitions($topic);
            $offsets = $this->metadata->getTopicOffsets($this->groupId, $topic);

            foreach ($partitions as $vpId => $virtualPartition) {
                $partition = $virtualPartition['partition'];
                $broker = $this->getKafkaByBrokerId($virtualPartition['broker']);
                $consumer = $broker->createConsumer();
                if (!isset($offsets[$virtualPartition['id']])) {
                    $offset = $offsetReset;
                } else {
                    $offset = $offsets[$virtualPartition['id']];
                    $ofsr = $consumer->offsets($topic,$partition, \Kafka\Kafka::OFFSETS_EARLIEST);
                    if ($offset < array_shift($ofsr)) {
                        $offset = $offsetReset;
                    } else {
                        $ofsr = $consumer->offsets($topic,$partition, \Kafka\Kafka::OFFSETS_LATEST);
                        if ($offset > array_shift($ofsr)) {
                            $offset = $offsetReset;
                        }
                    }
                }

                $stream = new MessageStream(
                    $this->metadata,
                    $this->groupId,
                    $consumer,
                    $topic,
                    $virtualPartition['broker'],
                    $partition,
                    $maxFetchSize,
                    $offset
                );
                $streams[] = $stream;
            }
        }
        $context->assignStreams($streams);
    }

    private function getOwnedPartitions($topic) {
        //TODO figure out which partitions we own within our group
        return $this->topicMetadata[$topic];
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
