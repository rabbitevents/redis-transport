<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use Illuminate\Support\Arr;
use RabbitEvents\Foundation\Contracts\Connection as ConnectionContract;
use RabbitEvents\Foundation\Contracts\Destination;
use RabbitEvents\Foundation\Contracts\FailedDestination;
use RabbitEvents\Foundation\Contracts\Producer;
use RabbitEvents\Foundation\Contracts\QueueConsumer;
use Redis;
use RuntimeException;
use Throwable;

class Connection implements ConnectionContract
{
    private ?RedisClientInterface $client = null;

    /**
     * @param array<string, mixed> $config
     * @param RedisClientInterface|null $client
     */
    public function __construct(
        private readonly array $config = [],
        ?RedisClientInterface $client = null
    ) {
        $this->client = $client;
    }

    public function getClient(): RedisClientInterface
    {
        if ($this->client === null) {
            $this->client = $this->createClient();
        }

        return $this->client;
    }

    public function createProducer(): Producer
    {
        return new RedisProducerAdapter($this->getClient());
    }

    public function makeConsumer(Destination $queue): QueueConsumer
    {
        if (!$queue instanceof RedisQueue) {
            throw new RuntimeException('Destination must be an instance of ' . RedisQueue::class);
        }

        $consumerName = $this->getConfig('consumer', gethostname() ?: 'default');

        return new RedisConsumerAdapter($this->getClient(), $queue, (string) $consumerName);
    }

    public function makeTopic(): Destination
    {
        $stream = $this->getConfig('stream', 'events');

        return new RedisTopic((string) $stream);
    }

    /**
     * @param string $queueName
     * @param array<string> $events
     * @param Destination|null $topic
     * @return Destination
     */
    public function makeQueue(string $queueName, array $events, ?Destination $topic = null): Destination
    {
        $topic = $topic ?: $this->makeTopic();
        $topic = $topic instanceof RedisTopic ? $topic : new RedisTopic((string) $topic->getOrigin());
        $queue = new RedisQueue($queueName, $events, $topic);

        // Only events published from now on; the direct stream holds only messages for this queue.
        $this->createGroup($topic->getName(), $queueName, '$');
        $this->createGroup($queue->getDirectStreamName(), $queueName, '0');

        return $queue;
    }

    public function makeFailedQueue(string $queueName): FailedDestination
    {
        $queue = new RedisFailedQueue($queueName);

        $this->createGroup($queue->getDirectStreamName(), $queue->getName(), '0');

        return $queue;
    }

    private function createGroup(string $stream, string $group, string $startId): void
    {
        try {
            // MKSTREAM creates the stream if it doesn't exist yet.
            $this->getClient()->xGroup('CREATE', $stream, $group, $startId, true);
        } catch (Throwable $e) {
            if (!str_contains($e->getMessage(), 'BUSYGROUP')) {
                throw $e;
            }
        }
    }

    public function getConfig(?string $key = null, mixed $default = null): mixed
    {
        if ($key !== null) {
            return Arr::get($this->config, $key, $default);
        }

        return $this->config;
    }

    protected function createClient(): RedisClientInterface
    {
        if (!extension_loaded('redis')) {
            throw new RuntimeException("The 'redis' PHP extension is required to use the Redis transport.");
        }

        $redis = new Redis();

        $host = (string) $this->getConfig('host', '127.0.0.1');
        $port = (int) $this->getConfig('port', 6379);
        $timeout = (float) $this->getConfig('timeout', 3.0);
        $password = $this->getConfig('password');
        $database = (int) $this->getConfig('database', 0);

        $redis->connect($host, $port, $timeout);

        if (!empty($password)) {
            $redis->auth((string) $password);
        }

        // Redis connects to database 0 by default; only issue SELECT for non-zero DBs to avoid network overhead
        if ($database > 0) {
            $redis->select($database);
        }

        return new PhpRedisClient($redis);
    }
}
