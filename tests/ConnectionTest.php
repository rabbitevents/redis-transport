<?php

declare(strict_types=1);

namespace RabbitEvents\Redis\Tests;

use Mockery as m;
use RabbitEvents\Foundation\Contracts\Connection as ConnectionContract;
use RabbitEvents\Redis\Connection;
use RabbitEvents\Redis\RedisClientInterface;
use RabbitEvents\Redis\RedisConsumerAdapter;
use RabbitEvents\Redis\RedisProducerAdapter;
use RabbitEvents\Redis\RedisQueue;
use RabbitEvents\Redis\RedisTopic;

class ConnectionTest extends TestCase
{
    public function test_implements_connection_contract(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $connection = new Connection(['stream' => 'my_events', 'consumer' => 'c1'], $client);

        self::assertInstanceOf(ConnectionContract::class, $connection);
        self::assertSame($client, $connection->getClient());
        self::assertEquals('my_events', $connection->getConfig('stream'));
    }

    public function test_create_producer(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $connection = new Connection([], $client);

        $producer = $connection->createProducer();

        self::assertInstanceOf(RedisProducerAdapter::class, $producer);
    }

    public function test_make_topic(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $connection = new Connection(['stream' => 'custom_stream'], $client);

        $topic = $connection->makeTopic();

        self::assertInstanceOf(RedisTopic::class, $topic);
        self::assertEquals('custom_stream', $topic->getName());
    }

    public function test_make_queue(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $connection = new Connection(['stream' => 'events'], $client);

        $topic = new RedisTopic('events');

        $client->shouldReceive('xGroup')
            ->once()
            ->with('CREATE', 'events', 'my-queue', '$', true)
            ->andReturn(true);

        $queue = $connection->makeQueue('my-queue', ['user.created'], $topic);

        self::assertInstanceOf(RedisQueue::class, $queue);
        self::assertEquals('my-queue', $queue->getName());
        self::assertEquals(['user.created'], $queue->getEvents());
    }

    public function test_make_consumer(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $connection = new Connection(['consumer' => 'worker-node-1'], $client);

        $topic = new RedisTopic('events');
        $queue = new RedisQueue('my-queue', ['user.*'], $topic);

        $consumer = $connection->makeConsumer($queue);

        self::assertInstanceOf(RedisConsumerAdapter::class, $consumer);
    }
}
