<?php

$time = microtime(true);
//bootstrap
chdir(dirname(__FILE__));
require "../src/Kafka.php";

$topic = "adviews";

$cc = new Kafka_ConsumerConnector("bl-queue-s01:2181");
$messageStreams = $cc->createMessageStreams($topic, 65535);

$counter = 0;

while (true)
{
    $fetchCount = 0;
    foreach ($messageStreams as $mid => $messageStream)
    {
            while ($message = $messageStream->nextMessage())
            {
                $counter++;
                $fetchCount ++;
                echo str_repeat(chr(8), 10) . str_pad($mid . ' ' . $counter, 10, ' ');
            }
            echo "\n";

    }
    if ($fetchCount == 0)
    {
        sleep(1);
        exit();
    }
}
echo "Counter = $counter\n";
