<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use RabbitEvents\Foundation\Contracts\TransportMessage;

class RedisTransportMessage implements TransportMessage
{
    /**
     * @param string $body
     * @param array<string, mixed> $properties
     * @param string|null $id The stream entry ID
     * @param string|null $stream The stream the entry was read from
     */
    public function __construct(
        private string $body,
        private array $properties = [],
        private ?string $id = null,
        private readonly ?string $stream = null
    ) {
    }

    public function getStream(): ?string
    {
        return $this->stream;
    }

    public function getId(): ?string
    {
        return $this->id;
    }

    public function setId(string $id): self
    {
        $this->id = $id;

        return $this;
    }

    public function getBody(): string
    {
        return $this->body;
    }

    public function setBody(string $body): self
    {
        $this->body = $body;

        return $this;
    }

    public function getProperty(string $name, mixed $default = null): mixed
    {
        return $this->properties[$name] ?? $default;
    }

    /**
     * @return array<string, mixed>
     */
    public function getProperties(): array
    {
        return $this->properties;
    }

    public function setProperty(string $name, mixed $value): void
    {
        $this->properties[$name] = $value;
    }

    public function getOrigin(): mixed
    {
        return $this->id;
    }

    public function getRoutingKey(): ?string
    {
        $event = $this->getProperty('event');

        return is_string($event) ? $event : null;
    }

    public function setRoutingKey(?string $key): void
    {
        $this->setProperty('event', $key);
    }

    public function getTimestamp(): ?int
    {
        $ts = $this->getProperty('timestamp');

        return is_numeric($ts) ? (int) $ts : null;
    }

    public function setTimestamp(int $timestamp): void
    {
        $this->setProperty('timestamp', $timestamp);
    }

    public function getMessageId(): ?string
    {
        $messageId = $this->getProperty('message_id');

        return is_string($messageId) && $messageId !== '' ? $messageId : $this->id;
    }

    public function setMessageId(?string $messageId): void
    {
        $this->setProperty('message_id', $messageId);
    }
}
