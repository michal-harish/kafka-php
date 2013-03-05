<?php
/**
 * Consumer Context represents a set of message streams that can be attached
 * some behaviour collectively. This is mainly for the purpose of implementing
 * the distributed rebalance algorithm
 *
 * @author    Michal Harish <michal.harish@gmail.com>
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @date 20/02/2013
 */

namespace Kafka;

class ConsumerContext implements \Iterator {

    private $streams = array();

    public function __destruct() {
        $this->close();
    }

    public function assignStreams(array $streams) {
        $this->streams = $streams;
    }

    public function close() {
        foreach($this->streams as $stream) {
            try {
                $stream->close();
            } catch (Exception $e) {
                //TODO enable some kind of logging
            }
        }
        $this->streams = array();
    }

    public function current () {
        return current($this->streams);
    }

    public function key () {
        return key($this->streams);
    }

    public function next () {
        return next($this->streams);
    }

    public function rewind () {
        return reset($this->streams);
    }

    public function valid () {
        return isset($this->streams[key($this->streams)]);
    }

}
