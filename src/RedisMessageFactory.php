<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use RabbitEvents\Foundation\Contracts\Payload;
use RabbitEvents\Foundation\Contracts\TransportMessage;
use RabbitEvents\Foundation\Contracts\TransportMessageFactory;

class RedisMessageFactory implements TransportMessageFactory
{
    /**
     * @param string $event
     * @param Payload $payload
     * @param array<string, mixed> $properties
     * @return TransportMessage
     */
    public function make(string $event, Payload $payload, array $properties = []): TransportMessage
    {
        $messageProperties = array_merge($properties, [
            'event' => $event,
            'content_type' => (string) $payload->contentType(),
            'content_encoding' => 'UTF-8',
        ]);

        return new RedisTransportMessage(
            $payload->serialize(),
            $messageProperties
        );
    }
}
