Kafka PHP
=========

This is an alternative to the existing Kafka PHP Client which is in the 
incubator, the main motivation to write it was that it seemed important
that the fetch requests are not loaded entirely into memory but pulled 
continuously from the socket as well as the fact that PHP has a different 
control flow and communication pattern (each request is a thread within 
HTTP server) so the api doesn't need to follow the scala/java object graph 
and can be much simpler.
 
There are few differences to the existing Kafka PHP Client:
     
    - Streaming message individually rather than loading the whole response 
      into memory
    - Offset implemented by hexdecimal tranformation to fully support Kafka 
      long offsets
    - Gzip working correctly both ways, including the pre-compression message 
      header
    - Messages produced in batch consumed correctly in compressed as well 
      as uncompressed state
    - CRC32 check working
    - Producers and Consumers are abstracted to allow for changes in Kafka 
      API without disrupting the client code
    - Broker abstraction for different connection strategies
    - OffsetRequest workaround for 64-bit unix timestamp
    - Produce request only checks correct bytes were sent (ack not available)
    - Producer compresses batches of consecutive messages with same 
      compression codec as a single message


Console Scripts
===============

There is a set of example scripts under the 'script' folder. The
parameters convention are shared across all the scripts but different
scripts has different parameters.

    -c  Connector, set which Zookeeper server you want to connect
    -b  Broker, the Kafka broker where we want to connect
    -t  Topic, sets the topic where you want to produce
    -m  Message, sets the message you want to produce
    -l  List, will list the available topics
    -o  Offset (optional), sets the starting point where
        we want to consume
    -h  Help, it will display the help for the script

Those are the available scripts:

    ./scripts/simple/producer -b {broker} -t {topc}
    ./scripts/simple/producer -b hq-pau-d02:9092 -t test-topic

    ./scripts/simple/consumer -b {broker} -t {topic} [-o {offset}]
    ./scripts/simple/consumer -b hq-pau-d02:9092 -t test-topic

    ./scripts/producers/producer -c {connector} -t {topic} -m {message}
    ./scripts/producers/producer -c hq-pau-d02:2181 -t test-topic -m "Hello"

    ./scripts/producers/cached -c {connector} -t {topic} -m {message}
    ./scripts/producers/cached -c hq-pau-d02:2181 -t test-topic -m "Hello"

    ./scripts/producers/partitioned -c {connector} -t {topic} -m {message}
    ./scripts/producers/partitioned -c hq-pau-d02:2181 -t test-topic -m "Hello"

    ./scripts/producers/daemon -c {connector} -t {topic}
    ./scripts/producers/daemon -c hq-pau-d02:2181 -t test-topic

    ./scripts/consumers/consumer -c {connector} -t {topic} 
    ./scripts/consumers/consumer -c hq-pau-d02:2181 -t test-topic 

    ./scripts/consumers/daemon -c {connector} -t {topic}
    ./scripts/consumers/daemon -c hq-pau-d02:2181 -t test-topic


Unit Tests
==========

Tests is a set of native PHP assert() calls included by the main runner:

    $> ./test


Tutorials
=========

This is not a tutorial, but will ilustrate how to create simple producer
and consumer, just to ilustrate how to use the kafka-php library.

Simple producer
---------------

This code will produce a message to the given topic.

    // require kafka-php library                            
    require "kafka-php/src/Kafka/Kafka.php";                
                                                            
    $connector = "hq-pau-d02:2181";                         
    $topic     = "test-topic";                              
    $message   = "Hello world!";                            
                                                            
    $producer = \Kafka\ProducerConnector::Create($connector);
                                                            
    // add the message                                      
    $producer->addMessage($topic, $message);                
                                                            
    // produce the actual messages into kafka               
    $producer->produce();                          


Simple consumer
---------------

This will show how to create a consumer and consume a single message.
Not that usefull, but will ilustrate the point.

    // require kafka-php library                           
    require "kafka-php/src/Kafka/Kafka.php";               
                                                           
    // setting variables                                   
    $connector = "hq-pau-d02:2181";                        
    $topic     = "test-topic";                             
                                                           
    // create the connector                                
    $cc = \Kafka\ConsumerConnector::Create($connector);    
                                                           
    // create the message stream, we point to the beginning
    // of the topic offset                                 
    $messageStream = $cc->createMessageStreams(            
        $topic,                                            
        65535,                                             
        \Kafka\Kafka::OFFSETS_EARLIEST                     
    );                                                     
                                                           
    // get the message                                     
    $message = $messageStream[0]->nextMessage();           
                                                           
    // output the message                                  
    echo $message->payload() ."\n";                        


Consume all messages from a topic 
---------------------------------

This consumer will do a similar thing, but will consume all messages
for a particular given topic, since the beginning (offset = 0).

    // require kafka-php library                                
    require "kafka-php/src/Kafka/Kafka.php";                    
                                                                
    // setting variables                                        
    $connector = "hq-pau-d02:2181";                             
    $topic     = "test-topic";                                  
                                                                
    // create the connector                                     
    $cc = \Kafka\ConsumerConnector::Create($connector);         
                                                                
    // create the message stream, we point to the beginning     
    // of the topic offset                                      
    $messageStreams = $cc->createMessageStreams(                
        $topic,                                                 
        65535,                                                  
        \Kafka\Kafka::OFFSETS_EARLIEST                          
    );                                                          
                                                                
    // infinite loop                                            
    while (true) {                                              
        $fetchCount = 0;                                        
                                                                
        foreach ($messageStreams as $mid => $messageStream) {   
            while ($message = $messageStream->nextMessage()) {  
                $fetchCount++;                                  
                echo $message->payload() . "\n";                
            }                                                   
        }                                                       
                                                                
        if ($fetchCount == 0) {                                 
            echo " --- no more messages ---\n";                 
            die;                                                
        }                                                       
    }                                                           


Consumer daemon
---------------

And finally, some closer to the real usage of this library. A consumer that 
will listen for new messages produced for a particular topic. The changes 
with regard to the previous consumers are that this time we ware going to 
set the highest possible offset, in order to ignore the past messages and 
only intercept the new ones.

    // require kafka-php library                                
    require "kafka-php/src/Kafka/Kafka.php";                    
                                                                
    // setting variables                                        
    $connector = "hq-pau-d02:2181";                             
    $topic     = "test-topic";                                  
                                                                
    // create the connector                                     
    $cc = \Kafka\ConsumerConnector::Create($connector);         
                                                                
    // create the message stream, we point to the end           
    // of the topic offset                                      
    $messageStreams = $cc->createMessageStreams(                
        $topic,                                                 
        65535,                                                  
        \Kafka\Kafka::OFFSETS_LATEST                            
    );                                                          
                                                                
    while (true) {                                              
        $fetchCount = 0;                                        
                                                                
        foreach ($messageStreams as $mid => $messageStream) {   
            // keep getting messages, if we have more           
            while ($message = $messageStream->nextMessage()) {  
                $fetchCount++;                                  
                // just print topic and payload                 
                echo "{$message->payload()}\n";                 
            }                                                   
        }                                                       
                                                                
        if ($fetchCount == 0) {                                 
            // no more messages, so sleep and try again         
            sleep(1);                                           
        }                                                       
    }                                                           


Backlog
=======

Those are the list of pending tasks:

 * IN PROGRESS - ConsumerConnector rebalance prcoess (zk watcher seems to have bugs so probably on nextMessage)
 * TODO - try implementing the new versioned wire format 0.8 and acknowledgements
 * TODO - Snappy compression - could not compile snappy.so on 64-bit :(
 * TODO - detect 64-bit php and replace Kafka_Offset hex for decimal under the hood

 * TODO - profiling & optimization
    - Channel - implement buffer in the hasIncomingData to speed-up the streaming and read from that buffer in the read() method
    - ConsumerChannel - profile consumption (decompression & descerialization cost, flushing broken response stream)
    - ProducerChannel - profile production (compression & serialization cost, )


Appendix - compiling php-zookeeper from source extension on ubuntu for apache2
==============================================================================
First prepare for compiling c sources and automake tools if you aren't

    sudo apt-get install build-essential checkinstall libcppunit-dev autoconf automake libtool ant

Then you'll need to compile the libzookeeper from the c sources

    sudo git clone git://github.com/apache/zookeeper.git /usr/share/zookeeper
    cd /usr/share/zookeeper/
    sudo ant compile_jute
    cd src/c
    ACLOCAL="aclocal -I /usr/local/share/aclocal" sudo autoreconf -if
    //OR//
    ACLOCAL="aclocal -I /usr/share/aclocal" sudo autoreconf -if
    sudo ./configure
    sudo make
    sudo make install

Clone php-zookeeper source and build php extension with phpize

    apt-get install php5-dev
    sudo git clone git://github.com/andreiz/php-zookeeper.git /usr/share/php-zookeeper
    cd /usr/share/php-zookeeper
    git checkout v0.2.1
    phpize
    sudo ./configure
    sudo make
    sudo make install
    sudo echo "extension=zookeeper.so" > /etc/php5/cli/conf.d/zookeeper.ini
    sudo echo "extension=zookeeper.so" > /etc/php5/apache2/conf.d/zookeeper.ini

Test if it works on cli and restart apache!

    echo '<?php $zoo = new Zookeeper("localhost:2181"); print_r($zoo->getChildren("/"));' | php
    service apache2 restart
