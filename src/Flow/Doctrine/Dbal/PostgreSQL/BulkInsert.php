<?php

declare(strict_types=1);

namespace Flow\Doctrine\Dbal\PostgreSQL;

use Doctrine\DBAL\Connection;
use Flow\Doctrine\Dbal\PostgreSQL\BulkInsert\BulkData;

final class BulkInsert
{
    private string $sqlFormat;

    private function __construct(string $sqlFormat)
    {
        $this->sqlFormat = $sqlFormat;
    }

    public static function upsertOnColumnConflict(string $table, string $columns) : self
    {
        return new self("
            INSERT INTO {$table} (%s)
            VALUES %s
            ON CONFLICT ({$columns}) 
            DO UPDATE SET %s
        ");
    }

    public static function insertOnConflictDoNothing(string $table) : self
    {
        return new self("
            INSERT INTO {$table} (%s)
            VALUES %s
            ON CONFLICT DO NOTHING 
        ");
    }

    public static function upsertOnConflictOnConstraint(string $table, string $constraint) : self
    {
        return new self("
            INSERT INTO {$table} (%s)
            VALUES %s
            ON CONFLICT ON CONSTRAINT {$constraint} 
            DO UPDATE SET %s
        ");
    }

    public static function insert(string $table) : self
    {
        return new self("
            INSERT INTO {$table} (%s)
            VALUES %s
        ");
    }

    public function execute(Connection $connection, BulkData $bulkData) : void
    {
        $connection->prepare(
            \sprintf(
                $this->sqlFormat,
                $bulkData->keys(),
                $bulkData->placeholders(),
                $bulkData->onConflictUpdate()
            ),
        )->execute(
            $bulkData->parameters()
        );
    }
}
