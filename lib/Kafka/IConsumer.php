<?php

/**
 * Interface for any consumers.
 * At this level the Kafka API version is transparent
 * to the client code. Different implementations
 * of this interface are provided for each version of Kafka API.
 * 
 * @author michal.harish@gmail.com
 */

interface Kafka_IConsumer
{
	/**
	 * @param Kafka $connection
	 */
	public function __construct(Kafka $connection);
	
	/**
	 * FetchRequest
	 * 
	 * @param string $topic
	 * @param int $partition
	 * @param Kafka_Offset $offset - Offset to fetch messages from
	 * @param int $maxMessageSize - Maximum size of a single message
	 * @throws Kafka_Exception
	 * @return bool Ready-to-read state
	 */
	public function fetch(
		$topic,
		$partition = 0,
		Kafka_Offset $offset = NULL,
		$maxMessageSize = 1000000
	);	
	
	/**
	 * The main method that pulls for messages
	 * to come downstream. If there are no more messages
	 * it will turn off the readable state and return FALSE
	 * so that next time another request is made to the connection
	 * if the program wants to check again later.
	 *
	 * @throws Kafka_Exception
	 * @return Kafka_Message | FLASE if no more messages
	 */
	public function nextMessage();

	/**
	 * The last offset position after connection or reading nextMessage().
	 * This value should be used for keeping the consumption state.
	 * It is different from the nextMessage()->getOffset() in that
	 * it points to the offset "after" that message.
	 *
	 * @return Kafka_Offset
	 */
	public function getWatermark();

	/**
	 * OffsetsRequest
	 * 
	 * @param string $topic
	 * @param int $partition
	 * @param mixed $time - can be unixtimestamp or hex offset
	 * @param int $maxNumOffsets
	 */
	public function offsets(
		$topic,
		$partition = 0,
		$time = Kafka::OFFSETS_LATEST,
		$maxNumOffsets = 2
	);
	
}