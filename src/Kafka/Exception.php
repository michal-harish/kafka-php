<?php

namespace Kafka;

class Exception extends \RuntimeException
{
}

namespace Kafka\Exception;

class EndOfStream extends \Kafka\Exception
{
}

class TopicUnavailable extends \Kafka\Exception
{
}