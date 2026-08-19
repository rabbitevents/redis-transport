<?php

declare(strict_types=1);

namespace RabbitEvents\Redis\Tests;

use Illuminate\Contracts\Foundation\Application;
use Mockery as m;
use RabbitEvents\Foundation\Connection\ConnectionFactory;
use RabbitEvents\Foundation\Connection\ConnectionManager;
use RabbitEvents\Redis\Connection;
use RabbitEvents\Redis\RedisTransportServiceProvider;

class RedisTransportServiceProviderTest extends TestCase
{
    public function test_registers_redis_driver_in_connection_manager(): void
    {
        /** @var Application&m\MockInterface $app */
        $app = m::mock(Application::class);
        $factory = new ConnectionFactory();
        $manager = new ConnectionManager($app, $factory);

        $app->shouldReceive('bound')->with(ConnectionManager::class)->andReturn(true);
        $app->shouldReceive('make')->with(ConnectionManager::class)->andReturn($manager);

        $provider = new RedisTransportServiceProvider($app);
        $provider->boot();

        $config = ['driver' => 'redis', 'stream' => 'events_test'];
        $connection = $factory->make($config);

        self::assertInstanceOf(Connection::class, $connection);
        self::assertEquals('events_test', $connection->getConfig('stream'));
    }
}
