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
            ->with('service:users', 'worker-1', ['events' => '>', 'rabbitevents:queue:service:users' => '>'], 1, 1000)
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
            ->with('service:users', 'worker-1', ['events' => '>', 'rabbitevents:queue:service:users' => '>'], 1, null)
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

        $message = new RedisTransportMessage('body', [], '1700000000-0', 'events');

        // Acknowledged in the stream it was read from.
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

        $message = new RedisTransportMessage('body', [], '1700000000-0', 'events');

        $client->shouldReceive('xAck')
            ->once()
            ->with('events', 'service:users', ['1700000000-0'])
            ->andReturn(1);

        $consumer->reject($message, false);
    }

    public function test_one_read_returning_entries_from_both_streams_loses_none(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $queue = new RedisQueue('service:users', ['user.*'], new RedisTopic('events'));
        $consumer = new RedisConsumerAdapter($client, $queue, 'worker-1');

        $client->shouldReceive('xReadGroup')->once()->andReturn([
            'events' => ['1-0' => ['body' => 'a', 'event' => 'user.created']],
            'rabbitevents:queue:service:users' => ['2-0' => ['body' => 'b', 'event' => 'user.updated']],
        ]);

        $first = $consumer->receive(0);
        $second = $consumer->receive(0);
        self::assertSame(['a', 'b'], [$first?->getBody(), $second?->getBody()]);
    }

    public function test_requeue_moves_messages_in_order_to_the_queue_direct_stream(): void
    {
        /** @var RedisClientInterface&m\MockInterface $client */
        $client = m::mock(RedisClientInterface::class);
        $queue = new RedisQueue('service:users', ['user.*'], new RedisTopic('events'));
        $consumer = new RedisConsumerAdapter($client, $queue, 'worker-1');
        $calls = [];

        $client->shouldReceive('xAdd')->twice()->andReturnUsing(function ($stream, $id, $fields) use (&$calls) {
            $calls[] = ['xAdd', $stream, $fields['body']];
            return '9-0';
        });
        $client->shouldReceive('xAck')->twice()->andReturnUsing(function ($stream, $group, $ids) use (&$calls) {
            $calls[] = ['xAck', $stream, $ids[0]];
            return 1;
        });

        $consumer->requeue([
            new RedisTransportMessage('a', ['event' => 'user.created'], '1-0', 'events'),
            new RedisTransportMessage('b', ['event' => 'user.updated'], '2-0', 'events'),
        ]);

        self::assertSame([
            ['xAdd', 'rabbitevents:queue:service:users', 'a'],
            ['xAck', 'events', '1-0'],
            ['xAdd', 'rabbitevents:queue:service:users', 'b'],
            ['xAck', 'events', '2-0'],
        ], $calls);
    }
}
