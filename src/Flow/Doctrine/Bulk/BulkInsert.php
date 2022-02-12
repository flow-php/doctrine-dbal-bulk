<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Connection;
use Flow\Doctrine\Bulk\QueryFactory\DbalQueryFactory;

final class BulkInsert
{
    private QueryFactory $queryFactory;

    public function __construct(QueryFactory $queryFactory)
    {
        $this->queryFactory = $queryFactory;
    }

    public static function create() : self
    {
        return new self(new DbalQueryFactory());
    }

    public function insert(Connection $connection, string $table, BulkData $bulkData) : void
    {
        $connection->executeQuery(
            $this->queryFactory->insert($connection->getDatabasePlatform(), $table, $bulkData),
            $bulkData->toSqlParameters(),
            \array_map(
                fn ($value) : string => \gettype($value),
                \array_filter($bulkData->toSqlParameters(), fn ($value) : bool => \is_bool($value))
            )
        );
    }

    public function insertOrSkipOnConflict(Connection $connection, string $table, BulkData $bulkData) : void
    {
        $connection->executeQuery(
            $this->queryFactory->insertOrSkipOnConflict($connection->getDatabasePlatform(), $table, $bulkData),
            $bulkData->toSqlParameters(),
            \array_map(
                fn ($value) : string => \gettype($value),
                \array_filter($bulkData->toSqlParameters(), fn ($value) : bool => \is_bool($value))
            )
        );
    }

    public function insertOrUpdateOnConstraintConflict(Connection $connection, string $table, string $constraint, BulkData $bulkData) : void
    {
        $connection->executeQuery(
            $this->queryFactory->insertOrUpdateOnConstraintConflict($connection->getDatabasePlatform(), $table, $constraint, $bulkData),
            $bulkData->toSqlParameters(),
            \array_map(
                fn ($value) : string => \gettype($value),
                \array_filter($bulkData->toSqlParameters(), fn ($value) : bool => \is_bool($value))
            )
        );
    }

    /**
     * @param Connection $connection
     * @param string $table
     * @param array<string> $conflictColumns
     * @param BulkData $bulkData
     * @param array<string> $updateColumns
     *
     * @throws \Doctrine\DBAL\Exception
     */
    public function insertOrUpdateOnConflict(Connection $connection, string $table, array $conflictColumns, BulkData $bulkData, array $updateColumns = []) : void
    {
        $connection->executeQuery(
            $this->queryFactory->insertOrUpdateOnConflict($connection->getDatabasePlatform(), $table, $conflictColumns, $bulkData, $updateColumns),
            $bulkData->toSqlParameters(),
            \array_map(
                fn ($value) : string => \gettype($value),
                \array_filter($bulkData->toSqlParameters(), fn ($value) : bool => \is_bool($value))
            )
        );
    }
}
