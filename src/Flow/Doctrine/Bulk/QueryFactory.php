<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Platforms\AbstractPlatform;

interface QueryFactory
{
    /**
     * @param AbstractPlatform $platform
     * @param string $table
     * @param BulkData $bulkData
     * @param array{
     *  skip_conflicts?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>
     * } $insertOptions $insertOptions
     *
     * @return string
     */
    public function insert(AbstractPlatform $platform, string $table, BulkData $bulkData, array $insertOptions = []) : string;
}
