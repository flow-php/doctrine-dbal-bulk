<?php

declare(strict_types=1);

namespace Flow\Doctrine\Tests;

use Doctrine\DBAL\DriverManager;
use Flow\Doctrine\Tests\Context\DatabaseContext;
use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    protected DatabaseContext $databaseContext;

    protected function setUp() : void
    {
        $this->databaseContext = new DatabaseContext(DriverManager::getConnection(['url' => \getenv('DATABASE_URL')]));
    }

    protected function tearDown() : void
    {
        $this->databaseContext->dropAllTables();
    }
}
