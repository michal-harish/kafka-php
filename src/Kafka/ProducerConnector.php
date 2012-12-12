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

abstract class ProducerConnector
{
    public static function Create(
        $connectionString,
        $compression = \Kafka\Kafka::COMPRESSION_NONE,
        Partitioner $partitioner = null,
        $apiVersion = 0.7
    ) {
        $apiImplementation = Kafka::getApiImplementation($apiVersion);
        include_once "{$apiImplementation}/ProducerConnector.php";
        $connectorClass = "\\Kafka\\{$apiImplementation}\\ProducerConnector";
        return new $connectorClass($connectionString, $compression, $partitioner);

    }

    public static function CreateCached(
        $connectionString,
        $compression = \Kafka\Kafka::COMPRESSION_NONE,
        Partitioner $partitioner = null,
        $apiVersion = 0.7
    ) {
        $apiImplementation = Kafka::getApiImplementation($apiVersion);
        include_once "{$apiImplementation}/ProducerConnector.php";

        $cacheFile = sys_get_temp_dir() . "/kafka-connector-{$apiVersion}-" . md5(serialize($connectionString));
        if (!file_exists($cacheFile) || time() - filemtime($cacheFile) > 60)
        {
            //create new connector and so redisover topics and brokers
            $connector = \Kafka\ProducerConnector::Create(
                $connectionString, $compression, $partitioner, $apiVersion
            );
            //and cache for another minute
            file_put_contents($cacheFile, serialize($connector));
        } else {
            $connector = unserialize(file_get_contents($cacheFile));
        }
        return $connector;
    }

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
    protected $topicPartitionMapping;


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
    private $brokerMapping;

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
    private $producerList;

    /**
     * Add message
     *
     * Method that will build a message given the payload, will randomly
     * decide which is the partition where we are going to send the
     * message and will send it.
     *
     * @param String $topic
     * @param String $payload
     * @param Partitioner|NULL $partitioner
     */
    public function addMessage(
        $topic,
        $payload,
        $key = null
    )
    {
        if (!array_key_exists($topic,$this->topicPartitionMapping))
        {
            throw new \Kafka\Exception(
                "Unknown Kafka topic `$topic`"
            );
        }

        //invoke partitioner
        $numPartitions = count($this->topicPartitionMapping[$topic]);
        $i = $this->partitioner->partition($key, $numPartitions);
        if (!is_integer($i) || $i<0 || $i>$numPartitions-1) 
        {
            throw new \Kafka\Exception(
                "Partitioner must return 0 <= integer < $numPartitions, returned $i"
            );
        }
        $partitionInfo = $this->topicPartitionMapping[$topic][$i];
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
    public function produce()
    {
        foreach ($this->producerList as $producer) {
            $producer->produce();
        }
    }

    /**
     * @return array of topic names
     */
    public function getAvailableTopics() {
        $result = array();
        foreach($this->topicPartitionMapping as $topic=>$partitions) {
            $result[] = "{$topic} [" . count($partitions). "]";
        }
        return $result;
    }

    /**
     * Get producer by broker id
     *
     * Method that given a broker id, it will create the producer and
     * will return it.
     *
     * @return IProducer
     */
    abstract protected function getProducerByBrokerId($brokerId);

    abstract public function __sleep();

    abstract public function __wakeup();

}
