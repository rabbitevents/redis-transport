<?php

declare(strict_types=1);

namespace RabbitEvents\Redis\Tests;

use RabbitEvents\Foundation\Contracts\ContentType;
use RabbitEvents\Foundation\Contracts\Payload;
use RabbitEvents\Redis\RedisMessageFactory;
use RabbitEvents\Redis\RedisTransportMessage;

class RedisMessageFactoryTest extends TestCase
{
    public function test_make_message(): void
    {
        $contentType = new class implements ContentType {
            public function getValue(): string
            {
                return 'application/json';
            }

            public function __toString(): string
            {
                return 'application/json';
            }
        };

        $payload = new class($contentType) implements Payload {
            public function __construct(private readonly ContentType $contentType)
            {
            }

            public function serialize(): string
            {
                return (string) json_encode(['foo' => 'bar']);
            }

            public function value(): mixed
            {
                return ['foo' => 'bar'];
            }

            public function contentType(): ContentType
            {
                return $this->contentType;
            }
        };

        $factory = new RedisMessageFactory();
        $message = $factory->make('item.created', $payload, ['custom_header' => 'header_value']);

        self::assertInstanceOf(RedisTransportMessage::class, $message);
        self::assertEquals(json_encode(['foo' => 'bar']), $message->getBody());
        self::assertEquals('item.created', $message->getRoutingKey());
        self::assertEquals('application/json', $message->getProperty('content_type'));
        self::assertEquals('UTF-8', $message->getProperty('content_encoding'));
        self::assertEquals('header_value', $message->getProperty('custom_header'));
    }
}
