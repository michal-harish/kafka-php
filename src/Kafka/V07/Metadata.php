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
        if ($this->zk == null) {
            $this->zk = new \Zookeeper($this->zkConnect);
            $this->brokerMetadata = null;
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
        if ($this->brokerMetadata === null) {
            $this->zkConnect();
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
        }
    }

    public function needsRefereshing() {
        return $this->brokerMetadata === null /*|| $this->topicMetadata === null*/;
    }

    /**
     * @param String $groupid
     * @param String $processId
     */
    public function registerConsumerProcess($groupId, $processId) {
        $this->zkConnect();
        $path = "/consumers/" . $groupId;
        if (!$this->zk->exists($path)) $this->createPermaNode($path); 
        if (!$this->zk->exists("$path/offsets")) $this->createPermaNode("$path/offsets");
        if (!$this->zk->exists("$path/owners")) $this->createPermaNode("$path/owners");
        if (!$this->zk->exists("$path/ids")) $this->createPermaNode("$path/ids");
        if (!$this->zk->exists("$path/ids/$processId")) $this->createEphemeralNode("$path/ids/$processId", "");
    }

    /**
     * Create null permanent node helper.
     * @param String $path
     */
    private function createPermaNode($path) {
        $this->zk->create(
            $path,
            null,
            $params = array(array(
				'perms'  => \Zookeeper::PERM_ALL,
				'scheme' => 'world',
				'id'     => 'anyone',
            ))
        );
    }

    /**
     * Create null permanent node helper.
     * @param String $path
     */
    private function createEphemeralNode($path, $value) {
        $this->zk->create(
            $path,
            $value,
            $params = array(array(
				'perms'  => \Zookeeper::PERM_ALL,
				'scheme' => 'world',
				'id'     => 'anyone',
            )),
            \Zookeeper::EPHEMERAL
        );
    }

}
