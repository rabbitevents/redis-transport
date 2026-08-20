<?php

declare(strict_types=1);

namespace RabbitEvents\Redis\Tests;

use Mockery as m;
use RabbitEvents\Foundation\Contracts\Producer;
use RabbitEvents\Redis\RedisClientInterface;
use RabbitEvents\Redis\RedisProducerAdapter;
use RabbitEvents\Redis\RedisQueue;
use RabbitEvents\Redis\RedisTopic;
use RabbitEvents\Redis\RedisTransportMessage;

class RedisProducerAdapterTest extends TestCase
{
    public function test_send_message_to_stream(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $producer = new RedisProducerAdapter($client);

        self::assertInstanceOf(Producer::class, $producer);

        $topic = new RedisTopic('events');
        $message = new RedisTransportMessage('{"id":123}', ['event' => 'user.registered', 'content_type' => 'application/json']);

        $client->shouldReceive('xAdd')
            ->once()
            ->with('events', '*', [
                'body' => '{"id":123}',
                'event' => 'user.registered',
                'properties' => json_encode(['event' => 'user.registered', 'content_type' => 'application/json']),
            ])
            ->andReturn('1700000000-0');

        $producer->send($topic, $message);
    }

    public function test_send_resolves_stream_name_from_redis_queue(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $producer = new RedisProducerAdapter($client);

        $topic = new RedisTopic('my-stream');
        $queue = new RedisQueue('my-consumer-group', ['user.*'], $topic);

        $message = new RedisTransportMessage('{"retry":true}', ['event' => 'user.created']);

        // Must XADD to "my-stream" (the underlying stream), NOT "my-consumer-group"
        $client->shouldReceive('xAdd')
            ->once()
            ->with('my-stream', '*', m::type('array'))
            ->andReturn('1700000001-0');

        $producer->send($queue, $message);
    }
}
