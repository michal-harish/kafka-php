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

    /**
     * Create null permanent node helper.
     * @param String $path
     */
    private function createPermaNode($path, $value = null) {
        $this->zk->create(
            $path,
            $value,
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
                        "id" => "{$brokerId}-{$p}",
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
        if (!$this->zk->exists("/consumers/{$groupId}")) $this->createPermaNode("/consumers/{$groupId}");
        //TODO if (!$this->zk->exists("/consumers/{$groupId}/owners")) $this->createPermaNode("/consumers/{$groupId}/owners");
        if (!$this->zk->exists("/consumers/{$groupId}/ids")) $this->createPermaNode("/consumers/{$groupId}/ids");

        if (!$this->zk->exists("/consumers/{$groupId}/ids/$processId")) {
            $this->createEphemeralNode("/consumers/{$groupId}/ids/$processId", "");
        }
    }

    /**
     * @param String $groupId
     * @param String $topic
     * @return array
     */
    public function getTopicOffsets($groupId, $topic) {
        $offsets = array();
        $this->zkConnect();
        if (!$this->zk->exists("/consumers/{$groupId}/offsets")) {
            $this->createPermaNode("/consumers/{$groupId}/offsets");
        }
        $path = "/consumers/{$groupId}/offsets/{$topic}";
        if ($this->zk->exists($path)) {
            foreach($this->zk->getChildren($path) as $partition) {
                $offsets[$partition] = new \Kafka\Offset($this->zk->get("{$path}/{$partition}"));
            }
        }
        return $offsets;
    }

    function commitOffset($groupId, $topic, $brokerId, $partition, \Kafka\Offset $offset) {
        $this->zkConnect();
        $path = "/consumers/{$groupId}/offsets/{$topic}";
        if (!$this->zk->exists($path)) {
            $this->createPermaNode($path);
        }
        if (!$this->zk->exists("{$path}/{$brokerId}-{$partition}")) {
            $this->createPermaNode("{$path}/{$brokerId}-{$partition}", $offset->__toString());
        } else {
            $this->zk->set("{$path}/{$brokerId}-{$partition}", $offset->__toString());
        }
    }

}
