<?php

/**
 * This class wraps the long format for message offset attributes
 * for 32bit php installations.
 *
 * @author michal.harish@gmail.com
 */

namespace Kafka;

class Offset_32bit extends Offset
{
    /**
     * the actual byte array of the value
     * @var string[8]
     */
    private $data;

    /**
     * Creating new offset can take initial hex value,
     * e.g new Offset("078c88cc700ff")
     *
     * @param string|int $fromString Hexadecimal string
     */
    public function __construct($fromString = null)
    {
        if (is_numeric($fromString)) {
	    	if (-1 == $fromString ) $fromString = "ffffffffffffffff"; //-1L
	    	else if (-2 == $fromString) $fromString = "fffffffffffffffe"; //-2L
	    	else $fromString = dechex($fromString);
        }
        $this->data = str_repeat(chr(0), 8);
        if ($fromString) {
            $this->data = $this->hexdata($fromString);
        }
    }

    /**
     * Creates an instance of an Offset from binary data
     * @param string $data
     */
    public function setData($data)
    {
    	$this->data = $data;    
    }
    

    /**
     * Print me
     */
    public function __toString()
    {
        $result = '';
        for ($i=0; $i < 8; $i++) {
            $result .= str_pad(
                dechex(ord($this->data[$i])),
                2,
                '0',
                STR_PAD_LEFT
            );
        }

        return $result;
    }

    /**
     * Return raw offset data.
     * @return string[8]
     */
    public function getData()
    {
        return $this->data;
    }

    /**
     * Increment offset by an integer
     * @param int $value
     */
    public function addInt($value)
    {
        $hex = dechex($value);
        $this->addData($this->hexdata(dechex($value)));
    }

    /**
     * Subtract integer from the offset
     * @param unknown_type $value
     */
    public function subInt($value)
    {
        $hex = dechex($value);
        $this->subData($this->hexdata(dechex($value)));
    }

    /**
     * Add an offset interval
     * @param Offset $value
     */
    public function add(Offset $value)
    {
        $this->addData($value->data);
    }

    /**
     * Subtract an offset interval
     * @param Offset $value
     */
    public function sub(Offset $value)
    {
        $this->subData($value->data);
    }

    /**
     * Internal parser for hex values
     * @param  string           $hex
     * @throws \Kafka\Exception
     */
    private function hexdata($hex)
    {
        $hex = str_pad($hex, 16, '0', STR_PAD_LEFT);
        $result = str_repeat(chr(0), 8);
        if (strlen($hex) != 16) {
            throw new \Kafka\Exception(
                'Hexadecimal offset cannot have more than 16 digits'
            );
        }
        for ($i=0; $i<8; $i++) {
            $h = substr($hex, $i * 2, 2);
            $result[$i] = chr(hexdec($h));
        }

        return $result;
    }

    /**
     * Internal addition that works with raw byte arrays
     * @param string[8] $add
     */
    private function addData($add)
    {
        $carry = 0;
        for ($i=7; $i>=0; $i--) {
            $dataByte = ord($this->data[$i]);
            $addByte = ord($add[$i]) + $carry;
            $resultByte = ($dataByte + $addByte) & 255;
            $carry = ($dataByte + $addByte) >> 8;
            $this->data[$i] = chr($resultByte);
        }
    }

    /**
     * Internal subtraction that works with raw byte arrays
     * @param string[8] $add
     */
    private function subData($sub)
    {
        $carry = 0;
        for ($i=7; $i>=0; $i--) {
            $dataByte = ord($this->data[$i]);
            $subByte = ord($sub[$i]) + $carry;
            $resultByte = (($dataByte - $subByte) + 256) & 255;
            $carry = - min($dataByte - $subByte, 0);
            $this->data[$i] = chr($resultByte);
        }
    }

}
