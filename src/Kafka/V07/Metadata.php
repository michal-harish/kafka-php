<?php

namespace Kafka\V07;

use Kafka\Kafka;

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
        if ($this->zk == null) {
            $this->zk = new \Zookeeper($this->zkConnect);
        }
    }

    public function getTopicMetadata()
    {
        $this->zkConnect();
        $topicMetadata = array();
        foreach ($this->zk->getChildren("/brokers/topics") as $topic) {
            foreach ($this->zk->getChildren("/brokers/topics/$topic") as $brokerId) {
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
        $this->zkConnect();
        // will return something like:
        // {something}-{numbers_that_looks_like_timestamp}:{host}:{port}
        if (!is_numeric($brokerId)) {
            throw new \Kafka\Exception("Invalid brokerId `$brokerId`");
        }
        $brokerIdentifier = $this->zk->get("/brokers/ids/$brokerId");

        $parts = explode(":", $brokerIdentifier );

        return array(
            'name' => $parts[0],
            'host' => $parts[1],
            'port' => $parts[2],
        );
    }

    public function getBrokerMetadata()
    {
        $this->zkConnect();
        $brokerMetadata = array();
        $brokers = $this->zk->getChildren("/brokers/ids");
        foreach ($brokers as $brokerId) {
            $brokerMetadata[$brokerId] = $this->getBrokerInfo($brokerId);
        }

        return $brokerMetadata;
    }

}
