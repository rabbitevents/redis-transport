<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use RabbitEvents\Foundation\Contracts\FailedDestination;

class RedisFailedQueue extends RedisQueue implements FailedDestination
{
    public function __construct(private readonly string $originalQueueName)
    {
        parent::__construct($originalQueueName . self::SUFFIX, [], null);
    }

    public function getOriginalQueueName(): string
    {
        return $this->originalQueueName;
    }
}
