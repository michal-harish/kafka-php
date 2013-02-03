<?php

namespace Kafka;

if (!defined('KAFKA_OFFSET_TYPE')) {
	if (PHP_INT_SIZE === 8) {
		define('KAFKA_OFFSET_CLASS', 64);
	} elseif (PHP_INT_SIZE === 4) {
		define('KAFKA_OFFSET_TYPE', 32);
	}
}


class Offset {
			
	private $component;
	
	public function __construct($fromString = null) {
		if (PHP_INT_SIZE === 8) {
			$this->component = new Offset_64bit($fromString);
		} elseif (PHP_INT_SIZE === 4) {
			$this->component = new Offset_32bit($fromString);
		}
	}
	
	public function setData($data) {
		$this->component->setData($data);
	}
	
	public function __toString() {
		return $this->component->__toString();
	}
	
	public function getData() {
		return $this->component->getData();
	}
	
	public function addInt($value) {
		$this->component->addInt($value);
	}
	
	public function subInt($value) {
		$this->component->subInt($value);
	}
	
	public function add(Offset $value) {
		$this->component->add($value);
	}
	
	public function sub(Offset $value) {
		$this->component->sub($value);
	}
	
	public function __clone() {
		if ($this->component != null) {
			$this->component = clone $this->component;
		}
	}
	
} 