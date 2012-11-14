<?php

class ConsumerConnector
{
    /**
     * Zookeeper Connection
     *
     * TODO
     */
    private $zk;

    /**
     * Kafka Broker list
     *
     * TODO
     */
    private $kafkaBrokerList;

    public function __construct($zkConnect)
    {
        $this->zk = new zookeeper($zkConnect);
    }

    /**
     * TODO
     *
     * @return Array Array containing the list of consumer streams
     */
    public function createMessageStreams($topic, $maxFetchSize = 1000)
    {
        $topicBrokers = $this->zk->getChildren("/brokers/topics/$topic");

        $messageStreams = array();

        foreach ($topicBrokers as $brokerId)
        {
            $partitionCount = $this->zk->get("/brokers/topics/$topic/$brokerId");

            $kafka = $this->getKafkaByBrokerId($brokerId);

            for ($partition = 0; $partition < $partitionCount; $partition++)
            {
                // $messageStreams[] = $kafka->createConsumer();
                $messageStreams[] = new MessageStream(
                    $kafka,
                    $topic,
                    $partition,
                    $maxFetchSize
                );
            }
        }

        return $messageStreams;
    }

    /**
     *
     * @return Kafka
     */
    private function getKafkaByBrokerId($brokerId)
    {
        if (!isset($this->brokerList[$brokerId]))
        {
            $tmp = $this->zk->get("/brokers/ids/$brokerId");

            // will return something like:
            // bl-queue-s03.visualdna.com-1352233014814:bl-queue-s03.visualdna.com:9092

            $parts = explode(":", $tmp);

            // loose the first part
            array_shift($parts);

            list($host, $port) = $parts;

            // instantiate the kafka broker representation
            $kafka = new Kafka($host, $port);

            //consumer
            $this->kafkaBrokerList[$brokerId] = $kafka;
        }

        return $this->kafkaBrokerList[$brokerId];
    }
}
