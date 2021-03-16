<?php

declare(strict_types=1);

namespace Flow\Doctrine\Tests\Integration\PostgreSQL;

use Flow\Doctrine\PostgreSQL\BulkInsert;
use Flow\Doctrine\Tests\IntegrationTestCase;

final class BulkInsertTest extends IntegrationTestCase
{
    public function test_inserts_multiple_rows_at_once() : void
    {
        $this->databaseContext->createTestTable($table = 'flow_doctrine_bulk_test');

        BulkInsert::insert($table)->execute($this->databaseContext->connection(), new BulkInsert\BulkData([
            ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
            ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
            ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three']
        ]));

        $this->assertEquals(3, $this->databaseContext->tableCount($table));
        $this->assertEquals(1, $this->databaseContext->numberOfExecutedInsertQueries());
    }

    public function test_insert_new_rows_or_updates_already_existed_based_on_primary_key() : void
    {
        $this->databaseContext->createTestTable($table = 'flow_doctrine_bulk_test');
        BulkInsert::insert($table)->execute($this->databaseContext->connection(), new BulkInsert\BulkData([
            ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
            ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
            ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three']
        ]));

        BulkInsert::upsertOnConflictOnConstraint($table, 'flow_doctrine_bulk_test_pkey')->execute($this->databaseContext->connection(), new BulkInsert\BulkData([
            ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
            ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
            ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three']
        ]));

        $this->assertEquals(4, $this->databaseContext->tableCount($table));
        $this->assertEquals(2, $this->databaseContext->numberOfExecutedInsertQueries());
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three']
            ],
            $this->databaseContext->selectAll($table)
        );
    }

    public function test_insert_new_rows_or_updates_already_existed_based_on_column() : void
    {
        $this->databaseContext->createTestTable($table = 'flow_doctrine_bulk_test');
        BulkInsert::insert($table)->execute($this->databaseContext->connection(), new BulkInsert\BulkData([
            ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
            ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
            ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three']
        ]));

        BulkInsert::upsertOnColumnConflict($table, 'id')->execute($this->databaseContext->connection(), new BulkInsert\BulkData([
            ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
            ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
            ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three']
        ]));

        $this->assertEquals(4, $this->databaseContext->tableCount($table));
        $this->assertEquals(2, $this->databaseContext->numberOfExecutedInsertQueries());
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three']
            ],
            $this->databaseContext->selectAll($table)
        );
    }

    public function test_insert_new_rows_and_skip_already_existed() : void
    {
        $this->databaseContext->createTestTable($table = 'flow_doctrine_bulk_test');
        BulkInsert::insert($table)->execute($this->databaseContext->connection(), new BulkInsert\BulkData([
            ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
            ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
            ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three']
        ]));

        BulkInsert::insertOnConflictDoNothing($table)->execute($this->databaseContext->connection(), new BulkInsert\BulkData([
            ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
            ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
            ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three']
        ]));

        $this->assertEquals(4, $this->databaseContext->tableCount($table));
        $this->assertEquals(2, $this->databaseContext->numberOfExecutedInsertQueries());
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three']
            ],
            $this->databaseContext->selectAll($table)
        );
    }
}
