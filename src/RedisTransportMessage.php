<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use RabbitEvents\Foundation\Contracts\TransportMessage;

class RedisTransportMessage implements TransportMessage
{
    /**
     * @param string $body
     * @param array<string, mixed> $properties
     * @param string|null $id
     */
    public function __construct(
        private string $body,
        private array $properties = [],
        private ?string $id = null
    ) {
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

    public function setRoutingKey(string $key): void
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

    public function getHeader(string $name, mixed $default = null): mixed
    {
        $headers = $this->getHeaders();

        return $headers[$name] ?? $default;
    }

    /**
     * @return array<string, mixed>
     */
    public function getHeaders(): array
    {
        $headers = $this->getProperty('headers', []);

        return is_array($headers) ? $headers : [];
    }

    public function setHeader(string $name, mixed $value): void
    {
        $headers = $this->getHeaders();
        $headers[$name] = $value;
        $this->setProperty('headers', $headers);
    }

    /**
     * @param array<string, mixed> $headers
     */
    public function setHeaders(array $headers): void
    {
        $this->setProperty('headers', $headers);
    }
}
