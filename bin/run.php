<?php

// Cross-platform test helper
// SECURITY: This script can only be run from CLI

if (php_sapi_name() !== 'cli') {
    fwrite(STDERR, "This script can only be run from the command line.\n");
    exit(1);
}

$rest = array_slice($argv, 1);
$cmdParts = [];

foreach ($rest as $arg) {
    if (empty($cmdParts) && str_contains($arg, '=')) {
        putenv($arg);
    } else {
        $cmdParts[] = $arg;
    }
}

$cmd = 'php ' . implode(' ', array_map('escapeshellarg', $cmdParts));
passthru($cmd, $code);
exit($code);
