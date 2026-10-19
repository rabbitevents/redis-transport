<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use Illuminate\Support\ServiceProvider;
use RabbitEvents\Foundation\Connection\ConnectionManager;

class RedisTransportServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        if ($this->app->bound(ConnectionManager::class)) {
            $this->app->make(ConnectionManager::class)->extend('redis', function (array $config, $app) {
                return new Connection($config);
            });
        }
    }
}
