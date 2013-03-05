<?php

/**
 * Interface for a low-level producer.
 *
 * At this level the Kafka API version is transparent
 * to the client code. Different implementations
 * of this interface are provided for each version of Kafka API.
 *
 * @author    Michal Harish <michal.harish@gmail.com>
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
*/

namespace Kafka;

interface IProducer
{
    /**
     * @param Kafka $connection
     */
    public function __construct(\Kafka\Kafka $connection);

    /**
     * Add message for production.
     * @param Message $message
     */
    public function add(Message $message);

    /**
     * Produce all messages added.
     * @throws \Kafka\Exception On Failure
     * @return TRUE             On Success
     */
    public function produce();


    public function close();

}
