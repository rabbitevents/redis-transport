<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

// Prefer a local RabbitEvents 10.x checkout over the copy in vendor/, ahead of Composer's classmap.
$candidatePaths = [
    '/var/www/rabbitevents/src/RabbitEvents',
    dirname(__DIR__, 3) . '/rabbitevents_10.x/src/RabbitEvents',
];

foreach ($candidatePaths as $root) {
    if (!is_dir("{$root}/Foundation")) {
        continue;
    }

    spl_autoload_register(static function (string $class) use ($root): void {
        foreach (['Foundation', 'Listener'] as $package) {
            $prefix = "RabbitEvents\\{$package}\\";
            if (str_starts_with($class, $prefix)) {
                $file = "{$root}/{$package}/" . str_replace('\\', '/', substr($class, strlen($prefix))) . '.php';
                if (is_file($file)) {
                    require $file;
                }
            }
        }
    }, true, true);

    break;
}
