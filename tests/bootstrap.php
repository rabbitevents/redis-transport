<?php

declare(strict_types=1);

$loader = require __DIR__ . '/../vendor/autoload.php';

$candidatePaths = [
    '/var/www/rabbitevents/src/RabbitEvents/Foundation',
    dirname(__DIR__, 3) . '/rabbitevents_9.x/src/RabbitEvents/Foundation',
];

foreach ($candidatePaths as $path) {
    if (is_dir($path)) {
        $loader->addPsr4('RabbitEvents\\Foundation\\', $path, true);
        $listenerPath = str_replace('Foundation', 'Listener', $path);
        if (is_dir($listenerPath)) {
            $loader->addPsr4('RabbitEvents\\Listener\\', $listenerPath, true);
        }
        break;
    }
}
