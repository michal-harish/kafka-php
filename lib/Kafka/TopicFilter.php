<?php
/**
 * 
 * This is a base for future topic filters and at the same time 
 * it is the basic implementation for currently supported exact match.
 *  
 * @author michal.harish@gmail.com
 */

class Kafka_TopicFilter
{
	/**
	 * @var string
	 */
	private $topic;
	
	/**
	 * @var int
	 */
	private $partition;
	
	/**
	 * @param string $topic
	 */
	public function __construct(
		$topic,
		$partition = 0
	)
	{
		$this->topic = $topic;
		$this->partition = $partition;
	}
	
	/**
	 * Write packet into a stream
	 * @param resource $connection
	 * @return int $written number of bytes succesfully sent
	 */
	public function writeTo($stream)
	{
		$written = fwrite($stream, pack('n', strlen($this->topic)));
		$written += fwrite($stream, $this->topic );
		$written += fwrite($stream, pack('N', $this->partition));
		return $written;
	}
	
	/**
	 * Return size of the packet for request size calculation
	 * @return number
	 */
	public function size()
	{
		return 2 + strlen($this->topic) + 4;
	}
} 