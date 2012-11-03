This is an alternative to the existing Kafka PHP Client which is in the incubator,
the main motivation to write it was that it seemed important
that the fetch requests are not loaded entirely into memory but
pulled continuously from the socket as well as the fact that php has a different control flow
and communication pattern (each request is a thread within http server)
so the api doesn't need to follow the scala/java object graph and can be much simpler.
 
    There are few differences to the existing Kafka PHP client: 
    - streaming message individually rather than loading the whole response into memory
    - offset implemented by hexdecimal tranformation to fully support Kafka long offsets 
    - gzip working correctly both ways, including the pre-compression message header
    - messages produced in batch consumed correctly in compressed as well as uncompressed state
    - crc32 check working
    - Producers and Consumers are abstracted to allow for changes in Kafka API without disrupting the client code 
    - Broker abstraction for different connection strategies
    - OffsetRequest workaround for 64-bit unix timestamp
    - Produce Request only checks correct bytes were sent (ack not available)
    - Producer compresses batches of consecutive messages with same compression codec as a single message 



Examples

/>php ./scripts/consumer.php  
/>php ./scripts/producer.php



TODO Batch producer (with compressed message set) - this would be useful for tail -f .. | type of producers 
TODO - profiling & optimization
    - Channel - implement buffer in the hasIncomingData to speed-up the streaming and read from that buffer in the read() method
    - ConsumerChannel - profile consumption (decompression & descerialization cost, flushing broken response stream)
    - ProducerChannel - profile production (compression & serialization cost, )
TODO Snappy compression     
    - could not compile snappy.so on 64-bit :(
TODO - implement the new versioned wire format 0.8 and acknowledgements 
    - waiting for a stable 0.8 candidate

