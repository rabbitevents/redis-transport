<?php

declare(strict_types=1);

namespace RabbitEvents\Redis\Tests;

use RabbitEvents\Foundation\Contracts\Destination;
use RabbitEvents\Redis\RedisQueue;
use RabbitEvents\Redis\RedisTopic;

class RedisDestinationAdapterTest extends TestCase
{
    public function test_topic_destination(): void
    {
        $topic = new RedisTopic('events');

        self::assertInstanceOf(Destination::class, $topic);
        self::assertEquals('events', $topic->getName());
        self::assertEquals('events', $topic->getOrigin());
    }

    public function test_queue_destination(): void
    {
        $topic = new RedisTopic('events');
        $queue = new RedisQueue('auth-service:user', ['user.created', 'user.*'], $topic);

        self::assertInstanceOf(Destination::class, $queue);
        self::assertEquals('auth-service:user', $queue->getName());
        self::assertEquals('auth-service:user', $queue->getOrigin());
        self::assertSame($topic, $queue->getTopic());
        self::assertEquals('events', $queue->getStreamName());
        self::assertEquals(['user.created', 'user.*'], $queue->getEvents());
    }
}
