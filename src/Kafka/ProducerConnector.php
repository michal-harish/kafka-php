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
        $connector = new $connectorClass($connectionString);
        $connector->compression = $compression;
        if ($partitioner === null) {
            $partitioner = new \Kafka\Partitioner();
        } elseif (!$partitioner instanceof \Kafka\Partitioner) {
            throw new \Kafka\Exception("partitioner must be instance of Partitioner class");
        }
        $connector->partitioner = $partitioner;
        return $connector;

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
    final public function addMessage(
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
    final public function produce()
    {
        foreach ($this->producerList as $producer) {
            $producer->produce();
        }
    }

    /**
     * @return array of topic names
     */
    final public function getAvailableTopics() {
        $result = array();
        foreach($this->topicPartitionMapping as $topic=>$partitions) {
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
     *
     * This method should be overriden by implementation class, e.g. in 0.7:
     * public function __sleep()
     * {
     *    return parent::__sleep() + array('zkConnect', 'brokerMapping');
     * }
     *
     * @return multitype:string
     */
    public function __sleep() {
        return array('topicPartitionMapping');
    }

    /**
     * Abstract constructor tales a connectionString which can
     * be zk.connect string or list of brokers or other metadata provider.
     * 
     * @param String $connectionString
     */
    abstract protected function __construct($connectionString);

    /**
     * Get producer by broker id
     *
     * Method that given a broker id, it will create the producer and
     * will return it.
     *
     * @return IProducer
     */
    abstract protected function getProducerByBrokerId($brokerId);

}
