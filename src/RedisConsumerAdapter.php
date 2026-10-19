<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use RabbitEvents\Foundation\Contracts\QueueConsumer;
use RabbitEvents\Foundation\Contracts\TransportMessage;
use Throwable;

class RedisConsumerAdapter implements QueueConsumer
{
    public function __construct(
        private readonly RedisClientInterface $client,
        private readonly RedisQueue $queue,
        private readonly string $consumerName
    ) {
    }

    /**
     * Entries already read from Redis but not returned yet: one read may return an entry per stream.
     *
     * @var list<RedisTransportMessage>
     */
    private array $buffer = [];

    public function receive(int $timeout = 0): ?TransportMessage
    {
        if (!$this->buffer) {
            $this->buffer = $this->read($timeout);
        }

        return array_shift($this->buffer);
    }

    public function acknowledge(TransportMessage $message): void
    {
        if ($message instanceof RedisTransportMessage && $message->getId() !== null) {
            $this->client->xAck(
                $message->getStream() ?? $this->queue->getDirectStreamName(),
                $this->queue->getName(),
                [$message->getId()]
            );
        }
    }

    public function reject(TransportMessage $message, bool $requeue = false): void
    {
        $requeue ? $this->requeue([$message]) : $this->acknowledge($message);
    }

    /**
     * A stream entry can't be put back, so a copy is appended to the queue's direct stream
     * (visible to this queue only) and the original is acknowledged. The order is kept.
     *
     * @param list<TransportMessage> $messages
     */
    public function requeue(array $messages): void
    {
        $producer = new RedisProducerAdapter($this->client);

        foreach ($messages as $message) {
            $producer->send($this->queue, $message);
            $this->acknowledge($message);
        }
    }

    /**
     * @return list<RedisTransportMessage>
     */
    private function read(int $timeout): array
    {
        $response = $this->client->xReadGroup(
            $this->queue->getName(),
            $this->consumerName,
            array_fill_keys($this->queue->getStreamNames(), '>'),
            1,
            $timeout > 0 ? $timeout : null // milliseconds, null means non-blocking
        );

        $messages = [];

        foreach ($response ?: [] as $stream => $entries) {
            foreach ($entries as $id => $fields) {
                $message = $this->toMessage((string) $stream, (string) $id, $fields);

                // The shared stream carries every event; skip the ones this queue doesn't listen to.
                if (!$this->eventMatches((string) $message->getRoutingKey(), $this->queue->getEvents())) {
                    $this->acknowledge($message);
                    continue;
                }

                $messages[] = $message;
            }
        }

        return $messages;
    }

    /**
     * @param array<string, string> $fields
     */
    private function toMessage(string $stream, string $id, array $fields): RedisTransportMessage
    {
        $properties = [];

        try {
            $decoded = json_decode($fields['properties'] ?? '[]', true, 512, JSON_THROW_ON_ERROR);
            $properties = is_array($decoded) ? $decoded : [];
        } catch (Throwable) {
        }

        $properties['event'] = $fields['event'] ?? $properties['event'] ?? '';

        return new RedisTransportMessage((string) ($fields['body'] ?? ''), $properties, $id, $stream);
    }

    /**
     * Check if an event matches any of the registered event wildcard patterns.
     *
     * @param string $event
     * @param array<string> $patterns
     * @return bool
     */
    protected function eventMatches(string $event, array $patterns): bool
    {
        if (empty($patterns)) {
            return true;
        }

        foreach ($patterns as $pattern) {
            if ($pattern === $event || $pattern === '#') {
                return true;
            }

            $escaped = preg_quote($pattern, '/');
            $regex = str_replace(['\\#', '\\*'], ['.*', '[^.]+'], $escaped);

            if (preg_match('/^' . $regex . '$/', $event)) {
                return true;
            }
        }

        return false;
    }
}
