<?php

/**
 * Offset 64 bit
 *
 * This class wraps the long format for message offset attributes
 * for 64bit php installations.
 *
 * @author    Michal Harish <michal.harish@gmail.com>
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 */

namespace Kafka;

class Offset_64bit extends Offset
{
    /**
     * the actual integer value of the offset
     * @var int
     */
    protected $int64;

    /**
     * Creating new offset can take initial hex value,
     * e.g new Offset("654654365465")
     *
     * @param string $fromString Decimal string
     */
    public function __construct($fromString = null)
    {
        $this->int64 = intval($fromString);
    }

    /**
     * Creates an instance of an Offset from binary data
     * @param string $data
     */
    public function setData($data)
    {
        $return = unpack('Na/Nb', $data);
        $this->int64 = ($return['a'] << 32) + ($return['b']);
    }

    /**
     * Print me
     */
    public function __toString()
    {
        return (string) $this->int64;
    }

    /**
     * Return raw offset data.
     * @return string[8]
     */
    public function getData()
    {
    $data = pack('NN', intval($this->int64) >> 32, $this->int64);

        return $data;
    }

    /**
     * Increment offset by an integer
     * @param int $value
     */
    public function addInt($value)
    {
        $this->int64 += $value;
    }

    /**
     * Subtract integer from the offset
     * @param unknown_type $value
     */
    public function subInt($value)
    {
        $this->int64 -= $value;
    }

    /**
     * Add an offset interval
     * @param Offset $value
     */
    public function add(Offset $value)
    {
        $this->addint($value->int64);
    }

    /**
     * Subtract an offset interval
     * @param Offset $value
     */
    public function sub(Offset $value)
    {
        $this->subInt($value->int64);
    }

}
