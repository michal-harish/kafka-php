<?php
class Kafka_Broker
{
    const MAGIC_0 = 0; //wire format without compression attribute
    const MAGIC_1 = 1; //wire format with compression attribute

	const REQUEST_KEY_PRODUCE      = 0;
	const REQUEST_KEY_FETCH        = 1;
	const REQUEST_KEY_MULTIFETCH   = 2;
	const REQUEST_KEY_MULTIPRODUCE = 3;
	const REQUEST_KEY_OFFSETS      = 4;
    
    const COMPRESSION_NONE = 0;
    const COMPRESSION_GZIP = 1;
    const COMPRESSION_SNAPPY = 2;
}