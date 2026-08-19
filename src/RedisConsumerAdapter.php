<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use RabbitEvents\Foundation\Contracts\QueueConsumer;
use RabbitEvents\Foundation\Contracts\TransportMessage;

class RedisConsumerAdapter implements QueueConsumer
{
    public function __construct(
        private readonly RedisClientInterface $client,
        private readonly RedisQueue $queue,
        private readonly string $consumerName
    ) {
    }

    public function receive(int $timeout = 0): ?TransportMessage
    {
        $stream = $this->queue->getStreamName();
        $group = $this->queue->getName();

        // Timeout in milliseconds (0 means non-blocking)
        $block = $timeout > 0 ? $timeout : null;

        $response = $this->client->xReadGroup(
            $group,
            $this->consumerName,
            [$stream => '>'],
            1,
            $block
        );

        if (!$response || !isset($response[$stream])) {
            return null;
        }

        foreach ($response[$stream] as $id => $fields) {
            $properties = [];
            if (isset($fields['properties'])) {
                try {
                    $decoded = json_decode($fields['properties'], true, 512, JSON_THROW_ON_ERROR);
                    if (is_array($decoded)) {
                        $properties = $decoded;
                    }
                } catch (\Throwable) {
                    $properties = [];
                }
            }

            $event = $fields['event'] ?? $properties['event'] ?? '';
            $properties['event'] = $event;

            // Check if this event matches the queue's subscribed events
            if (!$this->eventMatches($event, $this->queue->getEvents())) {
                // Acknowledge and skip message not destined for this handler
                $this->client->xAck($stream, $group, [(string) $id]);
                continue;
            }

            return new RedisTransportMessage(
                (string) ($fields['body'] ?? ''),
                $properties,
                (string) $id
            );
        }

        return null;
    }

    public function acknowledge(TransportMessage $message): void
    {
        $id = $message->getOrigin();

        if ($id !== null) {
            $this->client->xAck(
                $this->queue->getStreamName(),
                $this->queue->getName(),
                [(string) $id]
            );
        }
    }

    public function reject(TransportMessage $message, bool $requeue = false): void
    {
        if (!$requeue) {
            $this->acknowledge($message);
        }
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
            $regex = str_replace(['\#', '\*'], ['.*', '[^.]+'], $escaped);

            if (preg_match('/^' . $regex . '$/', $event)) {
                return true;
            }
        }

        return false;
    }
}
