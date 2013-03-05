<?php
/**
 * Default Partitioner for Producer Connector which accepts null or integer
 * keys and it will use random or simple modulo partitioning respectively.
 *
 * This class can be extended and partition($key, $numPartition) overriden
 * to accept any type of $key and method of partitioning.
 *
 * @author    Michal Harish <michal.harish@gmail.com>
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @date 10/12/2012
 */

namespace Kafka;

class Partitioner
{
    /**
     * @param  int|NULL $key
     * @param  int      $numPartitions
     * @return int      partition
     */
    public function partition($key, $numPartitions)
    {
        if ($key === null) {
            return rand(0, $numPartitions-1);
        } else {
            if (!is_integer($key)) {
                throw new \Kafka\Exception(
                    'Default Kafka Partitioner only accepts integer keys'
                );
            }

            return $key % $numPartitions;
        }
    }
}
