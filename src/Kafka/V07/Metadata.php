<?php

namespace Kafka\V07;

use Kafka\Kafka;
use Kafka\Message;
use Kafka\IProducer;

class Metadata implements \Kafka\IMetadata
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
     * @var null|array
     */
    private $brokerMetadata;

    /**
     * Construct
     */
    public function __construct($zkConnect)
    {
        $this->zkConnect = $zkConnect;
    }

    /**
     * Internal lazy connector for zookeeper.
     */
    private function zkConnect()
    {
        if ($this->zk == null)
        {
            $this->zk = new \Zookeeper($this->zkConnect);
            $this->brokerMetadata = null;
        }
    }

    public function getTopicMetadata() 
    {
        $this->zkConnect();
        $topicMetadata = array();
        foreach($this->zk->getChildren("/brokers/topics") as $topic) {
            foreach($this->zk->getChildren("/brokers/topics/$topic") as $brokerId) {
                $partitionCount = (int) $this->zk->get(
                    "/brokers/topics/$topic/$brokerId"
                );
                for ($p = 0; $p < $partitionCount; $p++) {
                    $topicMetadata[$topic][] = array(
                        "broker"    => $brokerId,
                        "partition" => $p,
                    );
                }
            }
        }
        return $topicMetadata;
    }

    public function getBrokerInfo($brokerId)
    {
        if (!$brokerId || !is_numeric($brokerId)) {
            throw new \Kafka\Exception("Invalid brokerId `$brokerId`");
        }
        $this->getBrokerMetadata();
        if (!isset($this->brokerMetadata[$brokerId])) {
            throw new \Kafka\Exception("Unknown brokerId `$brokerId`");
        }
        return $this->brokerMetadata[$brokerId];
    }

    public function getBrokerMetadata()
    {
        $this->zkConnect();
        if ($this->brokerMetadata === null) {
            $this->brokerMetadata = array();
            $brokers = $this->zk->getChildren("/brokers/ids", array($this,'brokerWatcher'));
            foreach($brokers as $brokerId)
            {
                $brokerIdentifier = $this->zk->get("/brokers/ids/$brokerId");
                $parts = explode(":", $brokerIdentifier);
                $this->brokerMetadata[$brokerId] = array(
                    'name' => $parts[0],
                	'host' => $parts[1],
                	'port' => $parts[2],
                );
            }
        }
        return $this->brokerMetadata;
    }

    public function brokerWatcher($type, $state, $path) {
        if ($path=="/brokers/ids") {
            $this->brokerMetadata = null;
            //$this->topicMetadata = null;
        }
    }

    public function needsRefereshing() {
        return $this->brokerMetadata === null /*|| $this->topicMetadata === null*/;
    }

}