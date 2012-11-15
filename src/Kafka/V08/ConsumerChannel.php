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

namespace Kafka\V08;

use Kafka\Kafka;
use Kafka\Message;
use Kafka\Offset;
use Kafka\IConsumer;

require_once realpath(dirname(__FILE__) . '/../V07/Channel.php');

class ConsumerChannel
    extends Channel
    // implements IConsumer
{
    // TODO
}
