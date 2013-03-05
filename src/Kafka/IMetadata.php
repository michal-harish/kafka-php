<?php
/* 
 * Metadata interface for accessing brokers, topics and consumer metadata.
 *
 * @author    Michal Harish <michal.harish@gmail.com>
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 */

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
    function getBrokerMetadata();

    /**
     * @return boolean
     */
    function needsRefereshing();


    /**
     * @param String $groupid
     * @param String $processId
     */
    function registerConsumerProcess($groupId, $processId);

    /**
     * @param String $groupId
     * @param String $topic
     */
    function getTopicOffsets($groupId, $topic);
    
    function commitOffset($groupId, $topic, $brokerId, $partition, \Kafka\Offset $offset);

}
