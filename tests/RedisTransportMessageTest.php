<?php

declare(strict_types=1);

namespace RabbitEvents\Redis\Tests;

use RabbitEvents\Foundation\Contracts\TransportMessage;
use RabbitEvents\Redis\RedisTransportMessage;

class RedisTransportMessageTest extends TestCase
{
    public function test_getters_and_setters(): void
    {
        $message = new RedisTransportMessage('body-content', ['event' => 'order.created', 'timestamp' => 1700000000], '1700000000-0');

        self::assertInstanceOf(TransportMessage::class, $message);
        self::assertEquals('body-content', $message->getBody());
        self::assertEquals('order.created', $message->getRoutingKey());
        self::assertEquals(1700000000, $message->getTimestamp());
        self::assertEquals('1700000000-0', $message->getId());
        self::assertEquals('1700000000-0', $message->getOrigin());
        self::assertEquals('order.created', $message->getProperty('event'));
        self::assertEquals('default', $message->getProperty('missing', 'default'));

        $message->setBody('new-body');
        self::assertEquals('new-body', $message->getBody());

        $message->setProperty('custom', 'val');
        self::assertEquals('val', $message->getProperty('custom'));

        $message->setTimestamp(1750000000);
        self::assertEquals(1750000000, $message->getTimestamp());

        $message->setId('new-id-1');
        self::assertEquals('new-id-1', $message->getId());
    }
}
