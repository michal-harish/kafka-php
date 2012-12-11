<?php
/**
 * Topic Filter for ConsumerConnector
 * There are two implementations
 * @author Michal Harish
 *
 */

namespace Kafka;

abstract class TopicFilter
{
    final public function getTopics(array $allTopics) {
        $resultTopics = array();
        foreach($allTopics as $topic) {
            if ($this->topicPassesFilter($topic)) {
                $resultTopics[] = $topic;
            }
        }
        return $resultTopics;
    }

    /**
     * @param String $topic
     * @return boolean 
     */
    abstract protected function topicPassesFilter($topic);

}

class Whitelist extends TopicFilter {
    private $regex;
    public function __construct($regex) {
        $this->regex = $regex;
    }
    protected function topicPassesFilter($topic) {
        return preg_match("/^{$this->regex}$/", $topic);
    }
}

class Blacklist extends TopicFilter {
    private $regex;
    public function __construct($regex) {
        $this->regex = $regex;
    }
    protected function topicPassesFilter($topic) {
        return !preg_match("/^{$this->regex}$/", $topic);
    }
}