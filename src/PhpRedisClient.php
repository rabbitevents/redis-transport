<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use Redis;

class PhpRedisClient implements RedisClientInterface
{
    public function __construct(private readonly Redis $redis)
    {
    }

    public function xAdd(string $stream, string $id, array $fields): string|false
    {
        return $this->redis->xAdd($stream, $id, $fields);
    }

    public function xGroup(string $operation, string $stream, string $group, string $id = '0', bool $mkstream = false): mixed
    {
        try {
            if ($operation === 'CREATE') {
                return $this->redis->xGroup('CREATE', $stream, $group, $id, $mkstream);
            }

            return $this->redis->xGroup($operation, $stream, $group, $id);
        } catch (\Throwable $e) {
            // Group already exists is normal in Redis Streams
            if (str_contains($e->getMessage(), 'BUSYGROUP')) {
                return true;
            }

            throw $e;
        }
    }

    public function xReadGroup(string $group, string $consumer, array $streams, ?int $count = 1, ?int $block = null): array|false
    {
        $countVal = $count ?? 1;
        $blockVal = $block ?? 0;

        $result = $this->redis->xReadGroup($group, $consumer, $streams, $countVal, $blockVal);

        return is_array($result) ? $result : false;
    }

    public function xAck(string $stream, string $group, array $ids): int
    {
        $result = $this->redis->xAck($stream, $group, $ids);

        return is_int($result) ? $result : 0;
    }

    public function xDel(string $stream, array $ids): int
    {
        $result = $this->redis->xDel($stream, $ids);

        return is_int($result) ? $result : 0;
    }

    public function ping(): bool|string
    {
        return $this->redis->ping();
    }

    public function getRedis(): Redis
    {
        return $this->redis;
    }
}
