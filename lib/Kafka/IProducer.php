<?php

/**
 * Interface for any producer.
 * At this level the Kafka API version is transparent
 * to the client code. Different implementations
 * of this interface are provided for each version of Kafka API.
 * 
 * @author michal.harish@gmail.com
 */

interface Kafka_IProducer
{
	/**
	 * @param Kafka $connection
	 */
	public function __construct(Kafka $connection);
	
	/**
	 * Add message for production.
	 * @param Kafka_Message $message
	 */
	public function add(Kafka_Message $message);
	
    /**
     * Produce all messages added.
     * @throws Kafka_Exception On Failure
     * @return TRUE On Success
     */
    public function produce();

}