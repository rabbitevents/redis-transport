# RabbitEvents Redis Streams Transport

[![Unit tests](https://github.com/rabbitevents/redis-transport/actions/workflows/testing.yml/badge.svg)](https://github.com/rabbitevents/redis-transport/actions/workflows/testing.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/eugene-nuwber/a86abd383c70df420b1fa23d130bc3b7/raw/redis-transport-coverage.json)](https://gist.github.com/eugene-nuwber/a86abd383c70df420b1fa23d130bc3b7)
[![Static code analysis](https://github.com/rabbitevents/redis-transport/actions/workflows/static-code-analysis.yml/badge.svg)](https://github.com/rabbitevents/redis-transport/actions/workflows/static-code-analysis.yml)
[![Total Downloads](https://img.shields.io/packagist/dt/rabbitevents/redis-transport)](https://packagist.org/packages/rabbitevents/redis-transport)
[![Latest Version](https://img.shields.io/packagist/v/rabbitevents/redis-transport)](https://packagist.org/packages/rabbitevents/redis-transport)
[![License](https://img.shields.io/packagist/l/rabbitevents/redis-transport)](https://packagist.org/packages/rabbitevents/redis-transport)  

Redis Streams transport driver for [RabbitEvents](https://github.com/nuwber/rabbitevents).

This package enables RabbitEvents to publish events to and consume events from **Redis Streams**, providing durable event streaming, consumer groups, acknowledgement (`XACK`), and retry handling.

---

## Requirements

* PHP >= 8.4
* `ext-redis` (recommended) or compatible client
* `rabbitevents/foundation` ^10.0

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

### Configuration Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `driver` | `string` | `redis` | The connection driver. |
| `host` | `string` | `127.0.0.1` | Redis server hostname or IP. |
| `port` | `int` | `6379` | Redis server port. |
| `password` | `string\|null` | `null` | Authentication password (if configured). |
| `database` | `int` | `0` | Redis logical database index (0-15). Default is `0`. |
| `timeout` | `float` | `3.0` | Connection timeout in seconds. |
| `stream` | `string` | `events` | The Redis Stream key name used as the event topic. |
| `consumer` | `string` | `hostname` | Unique consumer identifier within the consumer group. |

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

* **Topic**: a Redis Stream (`events` by default, config key `stream`) that publishers append every event to.
* **Queue**: a **consumer group** (`XGROUP`) on the topic stream, plus the queue's own **direct stream** `rabbitevents:queue:{queue}` with a group of the same name. A worker reads both with one `XREADGROUP`.
* **Messages for one queue** (retries released by a listener, messages put back with `requeue`, `rabbitevents:retry`) are appended to that queue's direct stream, so other consumer groups on the topic don't receive them.
* **Requeue**: a stream entry can't be returned, so `reject($message, requeue: true)` and `requeue($messages)` append a copy to the direct stream and acknowledge the original. Order is kept.
* **Acknowledgement**: `XACK` in the stream the entry was read from.
* **Wildcards**: AMQP-style patterns (`user.*`, `order.#`); entries of other events are acknowledged and skipped.

## Reliability & Failed Messages

* **Failed messages**: RabbitEvents 10.x moves a message that failed all its tries to `{queue}.failed`. With this transport it is a queue fed only through its direct stream (`rabbitevents:queue:{queue}.failed`); `rabbitevents:failed`, `retry`, `forget` and `flush` work as with RabbitMQ.
* **No delayed retries**: Redis Streams can't delay a delivery, so a released message is retried right away regardless of `--sleep`.
* **Message ID & Context Propagation**: RFC 4122 v4 UUID message IDs and Laravel `Context` propagation work as with RabbitMQ.
* **Deduplication**: transport-independent; see the optional [`rabbitevents/deduplication`](https://github.com/rabbitevents/deduplication) package.

---

## License

The MIT License (MIT). Please see [License File](LICENSE) for more information.
