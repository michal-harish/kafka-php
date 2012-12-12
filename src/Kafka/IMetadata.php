<?php

namespace Kafka;

interface IMetadata
{
    function __construct($connectionString);

    /**
     * @return array[<topic>][<virutalPartition>] = array('broker'= <brokerId>, 'partition' = <brokerPartition>)
     */
    function getTopicMetadata();

    /**
     * @param int $brokerId
     * @return array('name'=> ..., 'host'=>..., 'port' =>... )
     */
    function getBrokerInfo($brokerId);

    /**
     * @return array[<brokerId>] => array('name'=> ..., 'host'=>..., 'port' =>... )
     */
    function getBrokerMetadata();
}