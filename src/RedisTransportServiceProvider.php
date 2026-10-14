<?php

declare(strict_types=1);

namespace RabbitEvents\Redis;

use ArrayAccess;
use Illuminate\Support\ServiceProvider;
use RabbitEvents\Foundation\Connection\ConnectionManager;
use RabbitEvents\Listener\Deduplication\InMemoryDeduplicator;

class RedisTransportServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        if ($this->app->bound(ConnectionManager::class)) {
            $this->app->make(ConnectionManager::class)->extend('redis', function (array $config, $app = null) {
                $connection = new Connection($config);
                $container = $app ?: $this->app;

                $configRepo = ($container instanceof ArrayAccess && isset($container['config'])) ? $container['config'] : null;

                $strategyClass = $config['deduplication_strategy'] ?? ($configRepo?->get('rabbitevents.deduplication_strategy'));
                if ($strategyClass && class_exists($strategyClass)) {
                    $cacheSize = (int) ($config['deduplication_cache_size'] ?? ($configRepo?->get('rabbitevents.deduplication_cache_size', 5000) ?? 5000));
                    $deduplicator = ($strategyClass === InMemoryDeduplicator::class || $strategyClass === 'RabbitEvents\\Listener\\Deduplication\\InMemoryDeduplicator')
                        ? new InMemoryDeduplicator($cacheSize)
                        : (is_object($container) && method_exists($container, 'make') ? $container->make($strategyClass) : new $strategyClass());
                    $connection->setDeduplicationStrategy($deduplicator);
                }

                return $connection;
            });
        }
    }
}
