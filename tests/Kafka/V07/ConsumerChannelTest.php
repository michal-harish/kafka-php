<?php 

require_once __DIR__ . "/../../../src/Kafka/Kafka.php";

throw new Exception("FIXME test getWatermark doesn't advance when Kafka\Exception\EndOfStream encountered");
throw new Exception("FIXME test consumer offset advances correctly to the byte level after nextMessage()");
throw new Exception("FIXME test consumer offset doesn't advance when exception is raised during nextMessage()");
throw new Exception("FIXME test consumer offset doesn't advance nextMessage() returns null|false");