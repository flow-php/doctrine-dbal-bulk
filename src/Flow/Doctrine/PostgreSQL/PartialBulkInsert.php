<?php

declare(strict_types=1);

namespace Flow\Doctrine\PostgreSQL;

use Doctrine\DBAL\Connection;
use Flow\Doctrine\PostgreSQL\PartialBulkInsert\PartialBulkData;

final class PartialBulkInsert
{
    private $sqlFormat;

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

    public function execute(Connection $connection, PartialBulkData $partialBulkData) : void
    {
        $connection->prepare(
            \sprintf(
                $this->sqlFormat,
                $partialBulkData->keys(),
                $partialBulkData->placeholders(),
                $partialBulkData->onConflictUpdate()
            ),
        )->execute(
            $partialBulkData->parameters()
        );
    }
}
