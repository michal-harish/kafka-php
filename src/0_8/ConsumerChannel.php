<?php 
/**
 * Kafka 0.7 Producer Channel.
 * 
 * In this version there is no acknowledgement that the message has 
 * been received by the broker so the success is measured only
 * by writing succesfully into the socket.
 * 
 * The channel implementation, however is the same as in 0.7
 * 
 * @author michal.harish@gmail.com
 */

include_once realpath(dirname(__FILE__) . '/../0_7/Channel.php');

class Kafka_0_8_ConsumerChannel extends Kafka_0_7_Channel
//implements Kafka_IConsumer
{
	
}