# RabbitEvents Redis Streams Transport

[![Unit tests](https://github.com/rabbitevents/redis-transport/actions/workflows/testing.yml/badge.svg)](https://github.com/rabbitevents/redis-transport/actions/workflows/testing.yml)
[![Static code analysis](https://github.com/rabbitevents/redis-transport/actions/workflows/static-code-analysis.yml/badge.svg)](https://github.com/rabbitevents/redis-transport/actions/workflows/static-code-analysis.yml)

Redis Streams transport driver for [RabbitEvents](https://github.com/nuwber/rabbitevents).

This package enables RabbitEvents to publish events to and consume events from **Redis Streams**, providing durable event streaming, consumer groups, acknowledgement (`XACK`), and retry handling.

---

## Requirements

* PHP >= 8.4
* `ext-redis` (recommended) or compatible client
* `rabbitevents/foundation` >= 9.1

---

## Installation

```bash
composer require rabbitevents/redis-transport
```

---

## Configuration

Add the `redis` connection to your `config/rabbitevents.php`:

```php
return [
    'default' => env('RABBITEVENTS_CONNECTION', 'redis'),

    'connections' => [
        'redis' => [
            'driver' => 'redis',
            'host' => env('REDIS_HOST', '127.0.0.1'),
            'port' => env('REDIS_PORT', 6379),
            'password' => env('REDIS_PASSWORD', null),
            'database' => env('REDIS_DB', 0),
            'timeout' => env('REDIS_TIMEOUT', 3.0),
            'stream' => env('RABBITEVENTS_REDIS_STREAM', 'events'),
            'consumer' => env('RABBITEVENTS_REDIS_CONSUMER', gethostname() ?: 'default'),
        ],
        // ...
    ],
];
```

---

## Usage

### Publishing Events
Publishing events remains identical to standard RabbitEvents:

```php
use App\Events\UserRegistered;

event(new UserRegistered($user));
// or
publish('user.registered', ['user_id' => $user->id]);
```

### Listening to Events
Run the RabbitEvents worker with the Redis connection:

```bash
php artisan rabbitevents:listen --connection=redis
```

Or specify events directly:
```bash
php artisan rabbitevents:listen user.registered,order.* --connection=redis
```

---

## Architecture & Guarantees

* **Topic**: Mapped to a Redis Stream key (`events`).
* **Queue**: Mapped to a Redis Stream **Consumer Group** (`XGROUP`).
* **Reliability**: Uses `XADD` for publishing, `XREADGROUP` for receiving, and `XACK` on successful message processing.
* **Wildcards**: Supports AMQP-style routing key wildcards (`user.*`, `order.#`).

---

## License

The MIT License (MIT). Please see [License File](LICENSE) for more information.
