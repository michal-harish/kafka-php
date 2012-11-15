<?php

namespace Kafka;

class Exception
    extends \RuntimeException
{
}

namespace Kafka\Exception;

class EndOfStream
    extends \Kafka\Exception
{
}
