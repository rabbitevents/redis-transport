<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

interface RedisClientInterface
{
    /**
     * Append a message to a stream.
     *
     * @param string $stream
     * @param string $id
     * @param array<string, string> $fields
     * @return string|false
     */
    public function xAdd(string $stream, string $id, array $fields): string|false;

    /**
     * Manage consumer groups.
     *
     * @param string $operation
     * @param string $stream
     * @param string $group
     * @param string $id
     * @param bool $mkstream
     * @return mixed
     */
    public function xGroup(string $operation, string $stream, string $group, string $id = '0', bool $mkstream = false): mixed;

    /**
     * Read from a stream via consumer group.
     *
     * @param string $group
     * @param string $consumer
     * @param array<string, string> $streams
     * @param int|null $count
     * @param int|null $block
     * @return array<string, array<string, array<string, string>>>|false
     */
    public function xReadGroup(string $group, string $consumer, array $streams, ?int $count = 1, ?int $block = null): array|false;

    /**
     * Acknowledge messages in consumer group.
     *
     * @param string $stream
     * @param string $group
     * @param array<string> $ids
     * @return int
     */
    public function xAck(string $stream, string $group, array $ids): int;

    /**
     * Delete messages from a stream.
     *
     * @param string $stream
     * @param array<string> $ids
     * @return int
     */
    public function xDel(string $stream, array $ids): int;

    /**
     * Ping Redis server.
     *
     * @return bool|string
     */
    public function ping(): bool|string;
}
