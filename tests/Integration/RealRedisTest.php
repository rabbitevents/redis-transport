<?php

declare(strict_types=1);

namespace RabbitEvents\Redis\Tests\Integration;

use RabbitEvents\Listener\Support\Releaser;
use RabbitEvents\Redis\Connection;
use RabbitEvents\Redis\RedisTransportMessage;
use RabbitEvents\Redis\Tests\TestCase;
use Redis;
use RedisException;

/**
 * Runs against a real Redis server (REDIS_HOST, default "redis" from the integration docker-compose).
 */
class RealRedisTest extends TestCase
{
    private Connection $connection;

    protected function setUp(): void
    {
        parent::setUp();

        $host = getenv('REDIS_HOST') ?: 'redis';

        try {
            (new Redis())->connect($host, 6379, 1.0);
        } catch (RedisException) {
            self::markTestSkipped("Redis is not reachable at {$host}:6379.");
        }

        $this->connection = new Connection(['host' => $host, 'stream' => uniqid('events-'), 'consumer' => 'worker']);
    }

    public function test_message_sent_to_a_queue_reaches_only_that_queue(): void
    {
        $billing = $this->connection->makeQueue(uniqid('billing:'), ['order.created']);
        $mailer = $this->connection->makeQueue(uniqid('mailer:'), ['order.created']);
        $producer = $this->connection->createProducer();

        $producer->send($this->connection->makeTopic(), $this->message('order.created', 'published'));

        $billingConsumer = $this->connection->makeConsumer($billing);
        $mailerConsumer = $this->connection->makeConsumer($mailer);
        $this->assertSame('published', $billingConsumer->receive(100)?->getBody());
        $this->assertSame('published', $mailerConsumer->receive(100)?->getBody());

        // What Releaser and Failer do: send to the queue destination.
        $producer->send($billing, $this->message('order.created', 'retry'));

        // receive(200), not receive(100): PHPStan would reuse the type narrowed by the earlier identical call.
        $this->assertSame('retry', $billingConsumer->receive(200)?->getBody());
        $this->assertNull($mailerConsumer->receive(100), 'A retry of one queue must not reach other queues');
    }

    public function test_releasing_with_a_delay_does_not_break_the_redis_producer(): void
    {
        $queue = $this->connection->makeQueue(uniqid('svc:'), ['order.created']);
        $releaser = new Releaser($queue, $this->connection->createProducer());

        // rabbitevents:listen releases with --sleep=5 by default.
        $releaser->setDelay(5);
        $releaser->send(\RabbitEvents\Foundation\Message::createFromTransportMessage(
            new RedisTransportMessage('{"order_id":1}', ['event' => 'order.created', 'content_type' => 'application/json'])
        ));

        $this->assertSame('{"order_id":1}', $this->connection->makeConsumer($queue)->receive(100)?->getBody());
    }

    public function test_requeued_messages_are_delivered_again_in_order(): void
    {
        $failed = $this->connection->makeFailedQueue(uniqid('svc:'));
        $producer = $this->connection->createProducer();
        foreach (['first', 'second', 'third'] as $body) {
            $producer->send($failed, $this->message('order.created', $body));
        }

        // `rabbitevents:failed` browses the queue and returns what it didn't touch.
        $browser = $this->connection->makeConsumer($failed);
        $browsed = array_values(array_filter([$browser->receive(100), $browser->receive(100), $browser->receive(100)]));
        $this->assertCount(3, $browsed);
        $browser->requeue($browsed);

        // The next command run sees all of them again, in the same order.
        $next = $this->connection->makeConsumer($failed);
        $this->assertSame(
            ['first', 'second', 'third'],
            [$next->receive(100)?->getBody(), $next->receive(100)?->getBody(), $next->receive(100)?->getBody()]
        );
        $this->assertNull($next->receive(100));
    }

    private function message(string $event, string $body): RedisTransportMessage
    {
        return new RedisTransportMessage($body, ['event' => $event, 'content_type' => 'text/plain']);
    }
}
