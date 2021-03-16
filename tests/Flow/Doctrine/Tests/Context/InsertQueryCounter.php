<?php

declare(strict_types=1);

namespace Flow\Doctrine\Tests\Context;

use Doctrine\DBAL\Logging\SQLLogger;

final class InsertQueryCounter implements SQLLogger
{
    public int $count = 0;

    public function startQuery($sql, ?array $params = null, ?array $types = null)
    {
        if (\stripos(trim($sql), 'INSERT') === 0) {
            $this->count++;
        }
    }

    public function stopQuery()
    {
    }
}
