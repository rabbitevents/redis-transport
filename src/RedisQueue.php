<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use RabbitEvents\Foundation\Contracts\Destination;

/**
 * A consumer group reading the shared topic stream (published events) and its own direct stream
 * (messages addressed to this queue only: retries, requeues, failed messages).
 */
class RedisQueue implements Destination
{
    /**
     * @param string $name The consumer group name
     * @param array<string> $events The events to listen to
     * @param RedisTopic|null $topic The shared stream, or null for a queue fed only via its direct stream
     */
    public function __construct(
        private readonly string $name,
        private readonly array $events,
        private readonly ?RedisTopic $topic
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

    public function getTopic(): ?RedisTopic
    {
        return $this->topic;
    }

    public function getDirectStreamName(): string
    {
        return "rabbitevents:queue:{$this->name}";
    }

    /**
     * @return array<string>
     */
    public function getStreamNames(): array
    {
        return $this->topic
            ? [$this->topic->getName(), $this->getDirectStreamName()]
            : [$this->getDirectStreamName()];
    }

    public function getOrigin(): string
    {
        return $this->name;
    }
}
