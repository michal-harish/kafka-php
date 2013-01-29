<?php

/**
 * Kafka Producer Connector
 *
 * Producer Connector is the class that will provide the main methods to
 * spray Kafka Messages to all the active broker/partitions.
 *
 * @author     Pau Gay <pau.gay@gmail.com>
 * @date       2012-11-15
 *
 * The connector can be cached using serialize() function which will pack all the information
 * retreived from zookeeper so that things don't have to be rediscovered at every request, but
 * the application needs to decide how to do the actual caching.
 *
 * @author     Michal Hairsh <michal.harish@gmail.com>
 * @date 2012-12-03
 */

namespace Kafka;

class ProducerConnector
{

    /**
     * \Kafka\ProducerConnect::Create(...)
     *
     * @param  unknown_type     $connectionString
     * @param  unknown_type     $compression
     * @param  Partitioner      $partitioner
     * @param  unknown_type     $apiVersion
     * @throws \Kafka\Exception
     */
    public static function Create(
        $connectionString,
        $compression = \Kafka\Kafka::COMPRESSION_NONE,
        Partitioner $partitioner = null,
        $apiVersion = 0.7
    ) {
        $connector = new ProducerConnector($connectionString, $apiVersion);
        $connector->compression = $compression;
        if ($partitioner === null) {
            $partitioner = new \Kafka\Partitioner();
        } elseif (!$partitioner instanceof \Kafka\Partitioner) {
            throw new \Kafka\Exception("partitioner must be instance of Partitioner class");
        }
        $connector->partitioner = $partitioner;

        return $connector;

    }

    /**
     * \Kafka\ProducerConnect::CreateCached(...)
     *
     * @param  unknown_type     $connectionString
     * @param  unknown_type     $compression
     * @param  Partitioner      $partitioner
     * @param  unknown_type     $apiVersion
     * @throws \Kafka\Exception
     */
    public static function CreateCached(
        $connectionString,
        $compression = \Kafka\Kafka::COMPRESSION_NONE,
        Partitioner $partitioner = null,
        $apiVersion = 0.7
    ) {
        $cacheFile = sys_get_temp_dir() . "/kafka-connector-{$apiVersion}-" . md5(serialize($connectionString));
        if (!file_exists($cacheFile) || time() - filemtime($cacheFile) > 60) {
            //create new connector and so redisover topics and brokers
            $connector = \Kafka\ProducerConnector::Create($connectionString);
            //and cache for another minute
            file_put_contents($cacheFile, serialize($connector));
        } else {
            $connector = unserialize(file_get_contents($cacheFile));
        }
        $connector->compression = $compression;
        if ($partitioner === null) {
            $partitioner = new \Kafka\Partitioner();
        } elseif (!$partitioner instanceof \Kafka\Partitioner) {
            throw new \Kafka\Exception("partitioner must be instance of Partitioner class");
        }
        $connector->partitioner = $partitioner;

        return $connector;
    }

    private $connectionString;
    private $apiVersion;
    /**
    * @var \Kafka\IMetadata
    */
    private $metadata;

    /**
     * BrokerId - broker connector mapping
     *
     * Mapping that contains only connection argument for individual brokers,
     * but not actual socket handle.
     *
     * @format
     *     array(
     *         [brokerId] => array (
     *           "name" => "{kafkaname}",
     *           "host" => "{host}",
     *           "port" => "{port}",
     *         ),
     *         ...
     *     )
     * @var Array
     */
    protected $brokerMetadata;

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
    protected $topicMetadata;

    /**
     * @var int
     */
    protected $compression;

    /**
     * @var Partitioner
     */
    protected $partitioner;

    /**
     * Producer list
     *
     * List of Kafka Producer Channels that provide the connection to the
     * different partitions.
     *
     * @var Array of IProducer
     */
    protected $producerList;

    protected function __construct($connectionString, $apiVersion)
    {
        $this->connectionString = $connectionString;
        $this->apiVersion = $apiVersion;
        $this->refreshMetadata();
    }

    private function refreshMetadata()
    {
        if ($this->metadata == null) {
            $apiImplementation = Kafka::getApiImplementation($this->apiVersion);
            include_once "{$apiImplementation}/Metadata.php";
            $metadataClass = "\\Kafka\\{$apiImplementation}\\Metadata";
            $this->metadata = new $metadataClass($this->connectionString);
        }
        if($this->producerList) {
            foreach($this->producerList as $producer) {
                $producer->close();
            }
            $this->producerList = array();
        }
        $this->brokerMetadata = $this->metadata->getBrokerMetadata();
        $this->topicMetadata = $this->metadata->getTopicMetadata();
    }

    /**
     * Add message
     *
     * Method that will build a message given the payload, will randomly
     * decide which is the partition where we are going to send the
     * message and will send it.
     *
     * @param String           $topic
     * @param String           $payload
     * @param Partitioner|NULL $partitioner
     */
    final public function addMessage(
        $topic,
        $payload,
        $key = null
    )
    {
        if (!array_key_exists($topic,$this->topicMetadata))
        {
            if ($this->metadata !==null && $this->metadata->needsRefereshing()) {
                $this->refreshMetadata();
            }
        }
        if (!array_key_exists($topic,$this->topicMetadata))
        {
            throw new \Kafka\Exception\TopicUnavailable(
                "Kafka topic `$topic` not available"
            );
        }

        //invoke partitioner
        $numPartitions = count($this->topicMetadata[$topic]);

        $i = $this->partitioner->partition($key, $numPartitions);
        if (!is_integer($i) || $i<0 || $i>$numPartitions-1) {
            throw new \Kafka\Exception(
                "Partitioner must return 0 <= integer < $numPartitions, returned $i"
            );
        }
        $partitionInfo = $this->topicMetadata[$topic][$i];
        $brokerId  = $partitionInfo['broker'];
        $partition = $partitionInfo['partition'];

        // build the message
        $message = new Message(
            $topic,
            $partition,
            $payload,
            $this->compression
        );

        // get the actual producer we will add the mesasge
        if (!isset($this->producerList[$brokerId])) {
            $producer = $this->getProducerByBrokerId($brokerId);
            $this->producerList[$brokerId] = $producer;
        } else {
            $producer = $this->producerList[$brokerId];
        }

        $producer->add($message);
    }

    /**
     * Produce
     *
     * This method will actually produce the reall messages to Kafka.
     */
    final public function produce()
    {
        $failedQueue = array();
        foreach ($this->producerList as $producer) {
            try {
                $producer->produce();

            } catch (\Kafka\Exception $e) {
                foreach($producer->getMessageQueue() as $topic => $partitions) {
                    foreach($partitions as $partition => $messageSet ) {
                        foreach($messageSet as $message) {
                            $failedQueue[$topic][] = $message->payload();
                        } 
                    }
                }
            }
        }
        if ($failedQueue) {
            $this->refreshMetadata();
            foreach($failedQueue as $topic => $payloads) {
                foreach($payloads as $payload) {
                    $this->addMessage($topic, $payload);
                }
            }
        }
    }

    /**
     * @return array of topic names
     */
    final public function getAvailableTopics()
    {
        $result = array();
        foreach ($this->topicMetadata as $topic=>$partitions) {
            $result[] = "{$topic} [" . count($partitions). "]";
        }

        return $result;
    }

    /**
     * When waking up cached connector, we need to reset the handles so they
     * are initialized when required.
     */
    final public function __wakeup()
    {
        $this->producerList = array();
    }

    /**
     * @return multitype:string
     */
    final public function __sleep()
    {
        return array('topicMetadata','connectionString','apiVersion', 'brokerMetadata');
    }


    /**
     * Get producer by broker id
     *
     * Method that given a broker id, it will create the producer and
     * will return it.
     *
     * @return IProducer
     */
    protected function getProducerByBrokerId($brokerId)
    {
        if (!isset($this->producerList[$brokerId])) {
            if (!isset($this->brokerMetadata[$brokerId])) {
                throw new \Kafka\Exception(
                    "Broker connection paramters not initialized for broker $brokerId"
                );
            }
            $broker = $this->brokerMetadata[$brokerId];
            $kafka = new Kafka($broker['host'], $broker['port']);
            $this->producerList[$brokerId] = $kafka->createProducer();
        }

        return $this->producerList[$brokerId];
    }

}
