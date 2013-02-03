<?php

namespace Kafka;

interface IOffset {
	
	public function __toString();
	public function getData();
	public function addInt($value);
	public function subInt($value);
	public function add(Offset $value);
	public function sub(Offset $value);
	
} 