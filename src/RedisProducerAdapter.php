<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use RabbitEvents\Foundation\Contracts\Destination;
use RabbitEvents\Foundation\Contracts\Producer;
use RabbitEvents\Foundation\Contracts\TransportMessage;

class RedisProducerAdapter implements Producer
{
    public function __construct(private readonly RedisClientInterface $client)
    {
    }

    public function send(Destination $destination, TransportMessage $message): void
    {
        // A queue destination (release, retry, failed message) must reach only that queue,
        // so it goes to the queue's direct stream instead of the stream shared by all groups.
        $stream = $destination instanceof RedisQueue
            ? $destination->getDirectStreamName()
            : (string) $destination->getOrigin();

        $properties = $message->getProperties();

        $fields = [
            'body' => $message->getBody(),
            'event' => (string) ($message->getRoutingKey() ?? $properties['event'] ?? ''),
            'properties' => json_encode($properties, JSON_THROW_ON_ERROR),
        ];

        $this->client->xAdd($stream, '*', $fields);
    }
}
