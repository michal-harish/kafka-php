<?php 
/**
 * System test which checks that advancing of offsets can be handled cleanly when 
 * simple consumer encounters and exception while processing a message
 * and eventually gets all the messages from the stream. 
 */
require_once __DIR__ . "/../../src/Kafka/Kafka.php";

$result = "";
try {

	global $kafka;
    $consumer = $kafka->createConsumer();
    //get latest offset
    $offsets = $consumer->offsets("test", 0, \Kafka\Kafka::OFFSETS_LATEST);
    $watermark = $offsets[0];

    $producer = $kafka->createProducer();
    $producer->add(new \Kafka\Message("test", 0, "hello"));
    $producer->add(new \Kafka\Message("test", 0, "world"));
    $producer->produce();
    $producer->close();

    $thrown = false;
    while (true) {
        if ($consumer->fetch("test", 0, clone $watermark)) {
            while ($message = $consumer->nextMessage()) {
                $processed = FALSE;
                do {
                    try {
                        if (!$thrown) {
                            throw $thrown = new Exception(
                                'Simulation of consumer failure in a consistent state.'
                            );
                        }
                        $processed = TRUE;
                        $result .= $message->payload();
                    } catch (Exception $e) {}
                } while (!$processed);
            }
            break;
        }
    }
    $consumer->close();

} catch (Exception $e) {
    throw new SkippedException($e->getMessage());
}

assert($result == "helloworld");