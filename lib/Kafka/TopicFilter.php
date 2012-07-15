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
	 * Write packet into the request connection 
	 * @param resource $connection
	 */
	public function writeTo($connection)
	{
		fwrite(	$connection, pack('n', strlen($this->topic)));
		fwrite( $connection, $this->topic );
		fwrite( $connection, pack('N', $this->partition));
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