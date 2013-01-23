<?php

namespace Kafka;

interface IMetadata
{
    public function __construct($connectionString);

    /**
     * @return array[<topic>][<virutalPartition>] = array('broker'= <brokerId>, 'partition' = <brokerPartition>)
     */
    public function getTopicMetadata();

    /**
     * @param  int            $brokerId
     * @return array('name'=> ..., 'host'=>..., 'port' =>... )
     */
    public function getBrokerInfo($brokerId);

    /**
     * @return array[<brokerId>] => array('name'=> ..., 'host'=>..., 'port' =>... )
     */
    public function getBrokerMetadata();
}
