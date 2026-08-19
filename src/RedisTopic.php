<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use RabbitEvents\Foundation\Contracts\Destination;

class RedisTopic implements Destination
{
    public function __construct(private readonly string $name)
    {
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getOrigin(): string
    {
        return $this->name;
    }
}
