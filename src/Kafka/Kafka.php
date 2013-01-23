<?php

/**
 * Kafka connection object.
 *
 * Currently connects to a single broker, it can be later on extended to provide
 * an auto-balanced connection to the cluster of borkers without disrupting the
 * client code.
 *
 * @author michal.harish@gmail.com
 */

namespace Kafka;

require_once 'Exception.php';
require_once 'Offset.php';
require_once 'Message.php';
require_once 'IConsumer.php';
require_once 'IMetadata.php';
require_once 'IProducer.php';
require_once 'TopicFilter.php';
require_once 'ConsumerConnector.php';
require_once 'ProducerConnector.php';
require_once 'MessageStream.php';
require_once 'Partitioner.php';

class Kafka
{
    const MAGIC_0 = 0; // wire format without compression attribute
    const MAGIC_1 = 1; // wire format with compression attribute

    const REQUEST_KEY_PRODUCE      = 0;
    const REQUEST_KEY_FETCH        = 1;
    const REQUEST_KEY_MULTIFETCH   = 2;
    const REQUEST_KEY_MULTIPRODUCE = 3;
    const REQUEST_KEY_OFFSETS      = 4;

    const COMPRESSION_NONE = 0;
    const COMPRESSION_GZIP = 1;
    const COMPRESSION_SNAPPY = 2;

    const OFFSETS_LATEST = "ffffffffffffffff"; //-1L
    const OFFSETS_EARLIEST = "fffffffffffffffe"; //-2L

    // connection properties
    private $host;
    private $port;
    private $timeout;
    private $producerClass;
    private $consumerClass;

    /**
     * Constructor
     *
     * @param string $host
     * @param int    $port
     * @param int    $timeout
     * @param int    $kapiVersion Kafka API Version
     *     - the client currently recoginzes difference in the wire
     *    format prior to the version 0.8 and the versioned
     *    requests introduced in 0.8
     */
    public function __construct(
        $host = 'localhost',
        $port = 9092,
        $timeout = 6,
        $apiVersion = 0.7
    )
    {
        $this->host = $host;
        $this->port = $port;
        $this->timeout = $timeout;
        $apiImplementation = self::getApiImplementation($apiVersion);
        include_once "{$apiImplementation}/ProducerChannel.php";
        $this->producerClass = "\Kafka\\$apiImplementation\ProducerChannel";
        include_once "{$apiImplementation}/ConsumerChannel.php";
        $this->consumerClass = "\Kafka\\$apiImplementation\ConsumerChannel";
    }

    /**
     * @param  float  $apiVersion
     * @return string
     */
    public static function getApiImplementation($apiVersion)
    {
        if ($apiVersion < 0.8) {
            $apiImplementation = "V07";
        } elseif ($apiVersion < 0.9) {
            $apiImplementation = "V08";
        } else {
            throw new \Kafka\Exception(
                "Unsupported Kafka API version $apiVersion"
            );
        }

        return $apiImplementation;
    }

    /**
     * @return string "protocol://<host>:<port>";
     */
    public function getConnectionString()
    {
        return "tcp://{$this->host}:{$this->port}";
    }

    /**
     * @return int
     */
    public function getTimeout()
    {
        return $this->timeout;
    }

    /**
     * @return IProducer
     */
    public function createProducer()
    {
        $producerClass = $this->producerClass;

        return new $producerClass($this);
    }

    /**
     * @return IConsumer
     */
    public function createConsumer()
    {
        $consumerClass = $this->consumerClass;

        return new $consumerClass($this);
    }
}
