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
     * ZooKeeper Connection String, i.e. coma-separated list of host:port items
     * @var String
     */
    private $zkConnect;

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
     * Zookeeper Connection
     *
     * @var Zookeeper
     */
    private $zk;

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
        $this->zkConnect = $zkConnect;
        $this->discoverTopics();
        $this->discoverBrokers();
    }

    /**
     * This is for cached connectors - only the properties retreived from zookeeper 
     * are serialized but not actual connection handles.
     * 
     * @return multitype:string
     */
    public function __sleep()
    {
        return array('zkConnect', 'topicPartitionMapping', 'brokerMapping');
    }

    /**
     * When waking up cached connector, we need to reset the handles so they
     * are initialized when required.
     */
    public function __wakeup()
    {
        $this->zk = null;
        $this->producerList = array();
    }

    /**
     * Internal lazy connector for zookeeper.
     */
    private function zkConnect()
    {
        if ($this->zk == null)
        {
            $this->zk = new \Zookeeper($this->zkConnect);
        }
    }

    /**
     * Discover topics
     *
     * Method that will discover the Kafka topics stored in Zookeeper.
     * The method will populate the topicPartitionMapping array.
     */
    private function discoverTopics()
    {
        $this->zkConnect();

        // get the list of topics
        $topics = $this->zk->getChildren("/brokers/topics");

        $this->topicPartitionMapping = array();
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
     * Discover brokers' connection paramters.
     */
    private function discoverBrokers()
    {
        $this->zkConnect();

        $this->brokerMapping = array();
        $brokers = $this->zk->getChildren("/brokers/ids");
        foreach($brokers as $brokerId)
        {
            $brokerInfo = $this->zk->get("/brokers/ids/$brokerId");
            $parts = explode(":", $brokerInfo);
            list($name, $host, $port) = $parts;
            $this->brokerMapping[$brokerId] = array(
                'name' => $name,
                'host' => $host,
                'port' => $port,
            );
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
            if (!isset($this->brokerMapping[$brokerId]))
            {
                throw new \Kafka\Exception(
                	"Broker connection paramters not initialized for broker $brokerId"
                );
            }
            $broker = $this->brokerMapping[$brokerId];
            $kafka = new Kafka($broker['host'], $broker['port']);
            $this->producerList[$brokerId] = $kafka->createProducer();
        }

        return $this->producerList[$brokerId];
    }
}
