<?php 

throw new Exception("Test getWatermark doesn't advance when Kafka\Exception\EndOfStream encountered");
throw new Exception("Test consumer offset advances correctly to the byte level after nextMessage()");
throw new Exception("Test consumer offset doesn't advance when exception is raised during nextMessage()");
throw new Exception("Test consumer offset doesn't advance nextMessage() returns null|false");


