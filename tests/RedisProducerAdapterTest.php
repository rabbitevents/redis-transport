<?php

declare(strict_types=1);

namespace RabbitEvents\Redis\Tests;

use Mockery as m;
use RabbitEvents\Foundation\Contracts\Producer;
use RabbitEvents\Redis\RedisClientInterface;
use RabbitEvents\Redis\RedisProducerAdapter;
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
}
