<?php

declare(strict_types=1);

namespace RabbitEvents\Redis\Tests;

use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase as BaseTestCase;

class TestCase extends BaseTestCase
{
    use MockeryPHPUnitIntegration;
}
