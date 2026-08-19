<?php

declare(strict_types=1);

namespace RabbitEvents\Redis\Tests;

use Mockery as m;
use RabbitEvents\Foundation\Contracts\QueueConsumer;
use RabbitEvents\Redis\RedisClientInterface;
use RabbitEvents\Redis\RedisConsumerAdapter;
use RabbitEvents\Redis\RedisQueue;
use RabbitEvents\Redis\RedisTopic;
use RabbitEvents\Redis\RedisTransportMessage;

class RedisConsumerAdapterTest extends TestCase
{
    public function test_receive_matching_message(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $topic = new RedisTopic('events');
        $queue = new RedisQueue('service:users', ['user.*'], $topic);
        $consumer = new RedisConsumerAdapter($client, $queue, 'worker-1');

        self::assertInstanceOf(QueueConsumer::class, $consumer);

        $client->shouldReceive('xReadGroup')
            ->once()
            ->with('service:users', 'worker-1', ['events' => '>'], 1, 1000)
            ->andReturn([
                'events' => [
                    '1700000000-0' => [
                        'body' => '{"name":"John"}',
                        'event' => 'user.created',
                        'properties' => json_encode(['event' => 'user.created', 'content_type' => 'application/json']),
                    ],
                ],
            ]);

        $message = $consumer->receive(1000);

        self::assertInstanceOf(RedisTransportMessage::class, $message);
        self::assertEquals('{"name":"John"}', $message->getBody());
        self::assertEquals('user.created', $message->getRoutingKey());
        self::assertEquals('1700000000-0', $message->getId());
    }

    public function test_receive_returns_null_when_empty(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $topic = new RedisTopic('events');
        $queue = new RedisQueue('service:users', ['user.*'], $topic);
        $consumer = new RedisConsumerAdapter($client, $queue, 'worker-1');

        $client->shouldReceive('xReadGroup')
            ->once()
            ->with('service:users', 'worker-1', ['events' => '>'], 1, null)
            ->andReturn(false);

        $message = $consumer->receive(0);

        self::assertNull($message);
    }

    public function test_skips_and_acknowledges_unmatched_event(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $topic = new RedisTopic('events');
        $queue = new RedisQueue('service:users', ['order.*'], $topic);
        $consumer = new RedisConsumerAdapter($client, $queue, 'worker-1');

        $client->shouldReceive('xReadGroup')
            ->once()
            ->andReturn([
                'events' => [
                    '1700000000-0' => [
                        'body' => '{"name":"John"}',
                        'event' => 'user.created',
                    ],
                ],
            ]);

        $client->shouldReceive('xAck')
            ->once()
            ->with('events', 'service:users', ['1700000000-0'])
            ->andReturn(1);

        $message = $consumer->receive(0);

        self::assertNull($message);
    }

    public function test_acknowledge_message(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $topic = new RedisTopic('events');
        $queue = new RedisQueue('service:users', ['user.*'], $topic);
        $consumer = new RedisConsumerAdapter($client, $queue, 'worker-1');

        $message = new RedisTransportMessage('body', [], '1700000000-0');

        $client->shouldReceive('xAck')
            ->once()
            ->with('events', 'service:users', ['1700000000-0'])
            ->andReturn(1);

        $consumer->acknowledge($message);
    }

    public function test_reject_message_without_requeue(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $topic = new RedisTopic('events');
        $queue = new RedisQueue('service:users', ['user.*'], $topic);
        $consumer = new RedisConsumerAdapter($client, $queue, 'worker-1');

        $message = new RedisTransportMessage('body', [], '1700000000-0');

        $client->shouldReceive('xAck')
            ->once()
            ->with('events', 'service:users', ['1700000000-0'])
            ->andReturn(1);

        $consumer->reject($message, false);
    }
}
