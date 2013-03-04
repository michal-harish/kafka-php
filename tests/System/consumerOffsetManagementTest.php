<?php 
/**
 * System test for high level consumer can pick up lates offset from the zk
 * and start fetching there-off and commit the new offset on close.
 */

require_once __DIR__ . "/../../src/Kafka/Kafka.php";

try {
	global $kafka,$consumer;
    $producer = $kafka->createProducer();
    function fetch($streams, $count = null) {
        $messages = array();
        while(true) {
            foreach($streams as $stream) {
                while ($message = $stream->nextMessage()) {
                    $messages[] = $message;
                    if ($count !== null && count($messages) == $count) {
                        break;
                    }
                }
            }
            if ($count === null || count($messages) == $count) {
                break; 
            } else {
                usleep(250 * 1000);
            }
        }
        return $messages;
    }

    $streams = $consumer->createMessageStreams("test");

    fetch($streams);

    $u1 = uniqid();
    $producer->add(new \Kafka\Message("test", 0, $u1));
    $u2 = uniqid();
    $producer->add(new \Kafka\Message("test", 0, $u2));
    $producer->produce();
    $producer->close();

    $messages = fetch($streams,2);

    $streams->close();

} catch (Exception $e) {
    throw new SkippedException($e->getMessage());
}

assert(count($messages) == 2);
assert($messages[0]->payload() == $u1);
assert($messages[1]->payload() == $u2);


