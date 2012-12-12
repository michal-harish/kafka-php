<?php

namespace Kafka\V07;

use Kafka\Kafka;
use Kafka\Message;
use Kafka\IProducer;

class ProducerConnector extends \Kafka\ProducerConnector
{
    /**
     * ZooKeeper Connection String, i.e. coma-separated list of host:port items
     * @var String
     */
    private $zkConnect;

    /**
     * Zookeeper Connection
     *
     * @var Zookeeper
     */
    private $zk;

    /**
     * @param String $zkConnect
     * @param Integer $compression$compression
     * @param Partitioner|NULL $partitioner
     */
    protected function __construct(
        $zkConnect,
        $compression = \Kafka\Kafka::COMPRESSION_NONE,
        \Kafka\Partitioner $partitioner = null
    )
    {
        $this->zkConnect = $zkConnect;
        $this->compression = $compression;
        if ($partitioner === null) {
            $partitioner = new \Kafka\Partitioner();
        } elseif (!$partitioner instanceof \Kafka\Partitioner) {
            throw new \Kafka\Exception("partitioner must be instance of Partitioner class");
        }
        $this->partitioner = $partitioner;

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
        return array('topicPartitionMapping', 'brokerMapping', 'partitioner', 'zkConnect');
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
                $partitionCount = (int) $this->zk->get(
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