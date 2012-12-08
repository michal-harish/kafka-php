<?php

/**
 * Channel
 *
 * Generic Kafka 0.7 response-request channel.
 *
 * @author     Michal Haris <michal.harish@gmail.com>
 * @date       2012-11-15
 */

namespace Kafka\V07;

use Kafka\Offset;
use Kafka\Kafka;
use Kafka\Message;

abstract class Channel
{
    /**
     * Connection
     *
     * Connection object.
     *
     * @var Kafka
     */
    private $connection;

    /**
     * Socket
     *
     * Connection socket.
     *
     * @var Resource
     */
    protected $socket = null;

    /**
     * Socked send retry
     *
     * Number of times, a send operation should be attempted
     * when socket failure occurs.
     *
     * @var Integer
     */
    private $socketSendRetry = 1;

    /**
     * Rendable
     *
     * Request channel state.
     *
     * @var Boolean
     */
    private $readable;

    /**
     * Resnpose size
     *
     * Response of a readable channel.
     *
     * @var Integer
     */
    private $responseSize;

    /**
     * Read byets
     *
     * Number of bytes read from response.
     *
     * @var Integer
     */
    private $readBytes;

    /**
     * Inner stream
     *
     * Internal messageBatch for compressed sets of messages.
     *
     * @var Resource
     */
    private $innerStream = null;

    /**
     * Inner offset
     *
     * Internal for keeping the initial offset at which the innerStream starts
     * within the kafka stream.
     *
     * @var Offset
     */
    private $innerOffset = null;

    /**
     * Constructor
     *
     * @param Kafka $connection
     * @param String $topic
     * @param Integer $partition
     */
    public function __construct(\Kafka\Kafka $connection)
    {
        $this->connection = $connection;
        $this->readable = false;
        $this->socketSendRetry = 3;
    }

    /**
     * Destructor
     */
    public function __destruct()
    {
        $this->close();
    }

    /**
     * Close
     *
     * Close the connection(s). Must be called by the application
     * but could be added to the __destruct method too.
     */
    public function close()
    {
        $this->readable     = false;
        $this->responseSize = null;

        if (is_resource($this->socket)) {
            fclose($this->socket);
        }

        $this->socket = null;
    }

    /**
     * Create socket
     *
     * Set up the socket connection if not yet done.
     *
     * @throws \Kafka\Exception
     *
     * @return Resource $socket
     */
    protected function createSocket()
    {
        if (!is_resource($this->socket)) {
            $this->socket = stream_socket_client(
                $this->connection->getConnectionString(),
                $errno,
                $errstr
            );
            if (!$this->socket) {
                throw new \Kafka\Exception($errstr, $errno);
            }
            stream_set_timeout($this->socket, $this->connection->getTimeout());
            //stream_set_read_buffer($this->socket,  65535);
            //stream_set_write_buffer($this->socket, 65535);
        }
        return $this->socket;
    }

    /**
     * Send
     *
     * Send a bounded request.
     *
     * @param String $requestData
     * @param Boolean $expectsResponse
     *
     * @throws \Kafka\Exception
     */
    final protected function send($requestData, $expectsResposne = true)
    {
        $retry = $this->socketSendRetry;
        while ($retry > 0) {
            if ($this->socket === null) {
                $this->createSocket();
            }
            elseif ($this->socket === false) {
                throw new \Kafka\Exception(
                    "Kafka channel could not be created."
                );
            }
            if ($this->readable) {
                throw new \Kafka\Exception(
                    "Kafka channel has incoming data."
                );
            }
            $requestSize = strlen($requestData);
            $written = @fwrite($this->socket, pack('N', $requestSize));
            $written += @fwrite($this->socket, $requestData);
            if ($written  != $requestSize + 4) {
                $this->close();
                if (--$retry <= 0) {
                    throw new \Kafka\Exception(
                        "Request written $written bytes, expected to send:"
                        . ($requestSize + 4)
                    );
                } else {
                    continue;
                }
            }
            $this->readable = $expectsResposne;
            break;
        }

        return true;
    }

    /**
     * Read
     *
     * @param Integer  $size
     * @param Resource $stream
     *
     * @throws \Kafka\Exception
     */
    final protected function read($size, $stream = null)
    {
        if ($stream === null) {
            if (!$this->readable) {
                throw new \Kafka\Exception(
                    "Kafka channel is not readable."
                );
            }
            $stream = $this->socket;
        }
        if ($stream === $this->socket && $this->responseSize < $size) {
            //flush remaining data
            $this->read($this->responseSize, $stream);
            $this->readable = false;
            $remaining = $this->responseSize;
            $this->responseSize = null;
            throw new \Kafka\Exception\EndOfStream(
                "Trying to read $size from $remaining remaining."
            );
        }

        $soFarRead = 0;
        $result = '';
        $retrying = false;
        while ($soFarRead < $size) {
            $packet = fread($stream, $size - $soFarRead);
            if ($packet === false) {
                $this->close();
                throw new \Kafka\Exception(
                    "Could not read from the kafka channel socket."
                );
            } elseif (!$packet) {
                throw new \Kafka\Exception\EndOfStream(
                    "No response data received from kafka broker."
                );
            } else {
                $packetSize = strlen($packet);
                $soFarRead += $packetSize;
                $result .= $packet;
                if ($stream === $this->socket) {
                    $this->readBytes += $packetSize;
                    $this->responseSize -= $packetSize;
                }
            }
            $retrying = true;
        }
        return $result;
    }

    /**
     * Has incoming data
     *
     * Every response handler has to call this method to validate state of the
     * channel and read standard kafka channel headers.
     *
     * @throws \Kafka\Exception
     *
     * @return Boolean
     */
    protected function hasIncomingData()
    {
        if (is_resource($this->innerStream)) {
            $this->readBytes = 0;
            return true;
        }

        if ($this->socket === null) {
            $this->createSocket();
        } elseif ($this->socket === false) {
            throw new \Kafka\Exception(
                "Kafka channel could not be created."
            );
        }

        if (!$this->readable) {
            throw new \Kafka\Exception(
                "Request has not been sent - maybe a connection problem."
            );
            $this->responseSize = null;
        }

        if ($this->responseSize === null) {
            $bytes32 = @fread($this->socket, 4);
            if (!$bytes32) {
                $this->close();
                throw new \Kafka\Exception\EndOfStream(
                    "Could not read kafka response header."
                );
            }

            $this->responseSize = current(unpack('N', $bytes32));                       
            $errorCode = current(unpack('n', $this->read(2)));
            if ($errorCode != 0) {
                throw new \Kafka\Exception(
                    "Kafka response channel error code: $errorCode"
                );
            }
        }

        //has the request been read completely ?
        if ($this->responseSize < 0) {
            throw new \Kafka\Exception(
                "Corrupt response stream!"
            );
        } elseif ($this->responseSize == 0) {
            $this->readable = false;
            $this->responseSize = null;

            return false;
        } else {
            //TODO unit test readBytes do not get reset by any other method!
            //to ensure consitent advancing of the offset
            $this->readBytes = 0;

            return true;
        }
    }

    /**
     * Get read bytes
     *
     * @return Integer
     */
    public function getReadBytes()
    {
        return $this->readBytes;
    }

    /**
     * Get remaining bytes
     *
     * @return Integer
     */
    public function getRemainingBytes()
    {
        return $this->readBytes;
    }

    /**
     * Pack message
     *
     * Internal method for packing message into kafka wire format.
     *
     * @param Message $message
     * @param Mixed $overrideCompression Null or \Kafka\Kafka::COMPRESSION_NONE
     * or
     *      \Kafka\Kafka::COMPRESSION_GZIP, etc.
     *
     * @throws \Kafka\Exception
     */
    protected function packMessage(
        Message $message,
        $overrideCompression = null
    )
    {
        $compression = $overrideCompression === null
            ? $message->compression()
            : $overrideCompression;

        switch($compression) {
            case \Kafka\Kafka::COMPRESSION_NONE:
                $compressedPayload = $message->payload();
                break;
            case \Kafka\Kafka::COMPRESSION_GZIP:
                $compressedPayload = gzencode($message->payload());
                break;
            case \Kafka\Kafka::COMPRESSION_SNAPPY:
                throw new \Kafka\Exception(
                    "Snappy compression not yet implemented in php
                    client"
                );
                break;
            default:
                throw new \Kafka\Exception(
                    "Unknown kafka compression codec $compression"
                );
                break;
        }

        // for reach message using MAGIC_1 format which includes compression
        // attribute byte
        $messageBoundsSize = 1 + 1 + 4 + strlen($compressedPayload);
        $data = pack('N', $messageBoundsSize); //int
        $data .= pack('C', \Kafka\Kafka::MAGIC_1);//byte
        $data .= pack('C', $compression);//byte
        $data .= pack('N', crc32($compressedPayload));//int
        $data .= $compressedPayload;//unbounded string

        return $data;
    }

    /**
     * Load message
     *
     * Internal recursive method for loading a api-0.7  formatted message.
     *
     * @param unknown_type $topic
     * @param unknown_type $partition
     * @param Offset $offset
     * @param unknown_type $stream
     *
     * @throws \Kafka\Exception
     *
     * @return Message|false
     */
    protected function loadMessage(
        $topic,
        $partition,
        Offset $offset,
        $stream = null
    )
    {
        if (is_resource($this->innerStream)
            && $stream !== $this->innerStream
            && $innerMessage = $this->loadMessageFromInnerStream(
                $topic,
                $partition
            )
        ) {
            return $innerMessage;
        }

        if ($stream === null) {
            $stream = $this->socket;
        }

        if (!$size = @unpack('N', $this->read(4, $stream))) {
            return false;
        }

        $size = array_shift($size);

        if (!$magic = @unpack('C', $this->read(1, $stream))) {
            throw new \Kafka\Exception("Invalid Kafka Message");
        }

        switch($magic = array_shift($magic)) {
            case \Kafka\Kafka::MAGIC_0:
                $compression = \Kafka\Kafka::COMPRESSION_NONE;
                $payloadSize = $size - 5;
                break;
            case \Kafka\Kafka::MAGIC_1:
                $compression = array_shift(
                    unpack('C', $this->read(1, $stream))
                );
                $payloadSize = $size - 6;
                break;
            default:
                throw new \Kafka\Exception(
                    "Unknown message format - MAGIC = $magic"
                );
                break;
        }

        $crc32 = array_shift(unpack('N', $this->read(4, $stream)));

        switch($compression) {
            default:
                throw new \Kafka\Exception(
                    "Unknown kafka compression $compression"
                );
                break;
            case \Kafka\Kafka::COMPRESSION_SNAPPY:
                throw new \Kafka\Exception(
                    "Snappy compression not yet implemented in php client"
                );
                break;
            case \Kafka\Kafka::COMPRESSION_NONE:
                $payload = $this->read($payloadSize, $stream);
                if (crc32($payload) != $crc32) {
                    throw new \Kafka\Exception("Invalid message CRC32");
                }
                $compressedPayload = &$payload;
                break;
            case \Kafka\Kafka::COMPRESSION_GZIP:
                $gzHeader = $this->read(10, $stream);
                if (strcmp(substr($gzHeader, 0, 2), "\x1f\x8b")) {
                    throw new \Kafka\Exception('Not GZIP format');
                }
                $gzmethod = ord($gzHeader[2]);
                $gzflags = ord($gzHeader[3]);
                if ($gzflags & 31 != $gzflags) {
                    throw new \Kafka\Exception('Invalid GZIP header');
                }
                if ($gzflags & 1) {
                    // FTEXT
                    $ascii = true;
                }
                if ($gzflags & 4) {
                    // FEXTRA
                    $data = $this->read(2, $stream);
                    $extralen = array_shift(unpack("v", $data));
                    $extra = $this->read($extralen, $stream);
                    $gzHeader .= $data . $extra;
                }
                if ($gzflags & 8) {
                    // FNAME - zero char terminated string
                    $filename = '';
                    while (
                        ($char = $this->read(1, $stream))
                        && ($char != chr(0))
                    ) {
                        $filename .= $char;
                    }
                    $gzHeader .= $filename . chr(0);
                }
                if ($gzflags & 16) {
                    // FCOMMENT - zero char terminated string
                    $comment = '';
                    while (
                        ($char = $this->read(1, $stream))
                        && ($char != chr(0))
                    ) {
                        $comment .= $char;
                    }
                    $gzHeader .= $comment . chr(0);
                }
                if ($gzflags & 2) {
                    // FHCRC
                    $data = $this->read(2, $stream);
                    $hcrc = array_shift(unpack("v", $data));
                    if ($hcrc != (crc32($gzHeader) & 0xffff)) {
                        throw new \Kafka\Exception('Invalid GZIP header crc');
                    }
                    $gzHeader .= $data;
                }

                $payloadSize -= strlen($gzHeader);
                $gzData = $this->read($payloadSize - 8, $stream);
                $gzFooter = $this->read(8, $stream);
                $compressedPayload = $gzHeader . $gzData . $gzFooter;

                $apparentCrc32 = crc32($compressedPayload);
                if ($apparentCrc32 != $crc32) {
                    throw new \Kafka\Exception(
                        "Invalid compressed message CRC32 $crc32 <> "
                        . "$apparentCrc32"
                    );
                }

                $payloadBuffer = fopen('php://temp', 'rw');

                switch($gzmethod) {
                    case 0: //copy
                        $uncompressedSize = fwrite(
                            $payloadBuffer,
                            $gzData
                        );
                        break;
                    case 1: //compress
                        $uncompressedSize = fwrite(
                            $payloadBuffer,
                            gzuncompress($gzData)
                        );
                        break;
                    case 2: //pack
                        throw new \Kafka\Exception(
                            "GZip method unsupported: $gzmethod pack"
                        );
                        break;
                    case 3: //lhz
                        throw new \Kafka\Exception(
                            "GZip method unsupported: $gzmethod lhz"
                        );
                        break;
                    case 8: //deflate
                        $uncompressedSize = fwrite(
                            $payloadBuffer,
                            gzinflate($gzData)
                        );
                        break;
                    default :
                        throw new \Kafka\Exception(
                            "Unknown GZip method : $gzmethod"
                        );
                        break;
                }

                $datacrc = array_shift(unpack("V", substr($gzFooter, 0, 4)));
                $datasize = array_shift(unpack("V", substr($gzFooter, 4, 4)));
                rewind($payloadBuffer);
                if (
                    $uncompressedSize != $datasize
                    || crc32(stream_get_contents($payloadBuffer)) != $datacrc
                ) {
                    throw new \Kafka\Exception(
                        "Invalid size or crc of the gzip uncompressed data"
                    );
                }
                rewind($payloadBuffer);
                $this->innerStream = $payloadBuffer;
                $this->innerOffset = $offset;

                return $this->loadMessageFromInnerStream($topic, $partition);

                break;
        }

        $result =  new Message(
            $topic,
            $partition,
            $payload,
            $compression,
            $offset
        );

        return $result;
    }

    /**
     * Load message from inner stream
     *
     * Messages that arrive in compressed batches are stored in an internal
     * stream.
     *
     * @param unknown_type $topic
     * @param unknown_type $partition
     *
     * @return Message|null
     */
    private function loadMessageFromInnerStream($topic, $partition)
    {
        if (!is_resource($this->innerStream)) {
            throw new \Kafka\Exception("Invalid inner message stream");
        }

        try {
            if ($innerMessage = $this->loadMessage(
                    $topic,
                    $partition,
                    clone $this->innerOffset,
                    $this->innerStream
                )
            ) {
                return $innerMessage;
            }
        } catch (\Kafka\Exception\EndOfStream $e) {
            // finally
        }
        fclose($this->innerStream);
        $this->innerStream = null;
        return false;
    }
}
