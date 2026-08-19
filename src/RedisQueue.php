<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use RabbitEvents\Foundation\Contracts\Destination;

class RedisQueue implements Destination
{
    /**
     * @param string $name The consumer group name
     * @param array<string> $events The events to listen to
     * @param RedisTopic $topic The stream topic
     */
    public function __construct(
        private readonly string $name,
        private readonly array $events,
        private readonly RedisTopic $topic
    ) {
    }

    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return array<string>
     */
    public function getEvents(): array
    {
        return $this->events;
    }

    public function getTopic(): RedisTopic
    {
        return $this->topic;
    }

    public function getStreamName(): string
    {
        return $this->topic->getName();
    }

    public function getOrigin(): string
    {
        return $this->name;
    }
}
