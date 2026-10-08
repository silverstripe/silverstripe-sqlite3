<?php

/**
 * Backport upstream test fixes for old framework releases selected by --prefer-lowest.
 * Only vendor test assertions/setup are changed; already fixed tests are left alone.
 * Date fixes: silverstripe/silverstripe-framework 5.3.0, tests/php/{Forms,ORM}.
 * YAML fix: silverstripe/silverstripe-framework 5.4.30, tests/php/i18n/YamlReaderTest.php.
 * These literal replacements are idempotent and run before PHPUnit loads the tests.
 */
return static function (string $directory): array {
    $locale = ["i18n::set_locale('en_NZ');" => "i18n::set_locale('en_US');"];
    $patches = [
        'Forms/DateFieldDisabledTest.php' => $locale + [
            '1/02/2011 (today)' => 'Feb 1, 2011 (today)',
            '27/01/2011, 5 days ago' => 'Jan 27, 2011, 5 days ago',
            '6/02/2011, in 5 days' => 'Feb 6, 2011, in 5 days',
        ],
        'Forms/DatetimeFieldTest.php' => $locale + [
            "->setLocale('en_NZ');\n\n        \$datetimeField->setSubmittedValue('29/03/2003 11:00:00 pm')"
                => "->setLocale('de_DE');\n\n        \$datetimeField->setSubmittedValue('29/03/2003 23:00:00')",
            '#29/03/2003(,)? 11:00:00 (PM|pm)#' => '#29.03.2003(,)? 23:00:00#',
        ],
        'ORM/DBDateTest.php' => $locale + [
            "'31/03/2008'" => "'Mar 31, 2008'",
            "'30/03/2008'" => "'Mar 30, 2008'",
            "'4/03/2003'" => "'Mar 4, 2003'",
            "'31 March 2008'" => "'March 31, 2008'",
            "'30 March 2008'" => "'March 30, 2008'",
            "'3 April 2003'" => "'April 3, 2003'",
            "'Monday, 31 March 2008'" => "'Monday, March 31, 2008'",
        ],
        'ORM/DBDatetimeTest.php' => [
            // Keep the two-locale check in testNice(), as in the upstream fix.
            "i18n::set_locale('en_NZ');\n"
                . "        \$this->assertMatchesRegularExpression('#11/12/2001(,)? 10:10 PM#i'"
                => "i18n::set_locale('de_DE');\n"
                . "        \$this->assertMatchesRegularExpression('#11.12.2001(,)? 22:10#i'",
        ] + $locale + [
            '#Dec 11(,)? 2001(,)? 10:10 PM#i' => '#Dec 11(,)? 2001(,)? 10:10\hPM#iu',
            "'31/12/2001'" => "'Dec 31, 2001'",
            '#10:10:59 PM#i' => '#10:10:59\hPM#iu',
        ],
        'ORM/DBTimeTest.php' => [
            '#5:15:55 PM#i' => '#5:15:55\hPM#iu',
            '#5:15 PM#i' => '#5:15\hPM#iu',
        ],
        'i18n/YamlReaderTest.php' => [
            ' line 5 @' => ' line \d+ @',
            // Escape Windows path separators too; retain the same exception/path assertion.
            "str_replace('.', '\\.', \$path ?? '')" => "preg_quote(\$path ?? '', '@')",
        ],
        'ORM/SQLSelectTest.php' => [
            "public function testOrderByMultiple()\n    {\n        if (DB::get_conn() instanceof MySQLDatabase) {"
                => "public function testOrderByMultiple()\n    {\n"
                . "        if (!(DB::get_conn() instanceof MySQLDatabase)) {\n"
                . "            \$this->markTestSkipped('This test requires MySQL.');\n"
                . "        }\n        if (DB::get_conn() instanceof MySQLDatabase) {",
        ],
    ];

    $changed = [];
    foreach ($patches as $file => $replacements) {
        $path = $directory . '/' . $file;
        if (!is_file($path)) {
            continue;
        }
        $source = file_get_contents($path);
        // Match multi-line snippets on Windows too, while preserving existing line endings.
        $lineEnding = str_contains($source, "\r\n") ? "\r\n" : "\n";
        $patched = str_replace("\r\n", "\n", $source);
        foreach ($replacements as $before => $after) {
            $patched = str_replace($before, $after, $patched);
        }
        $patched = str_replace("\n", $lineEnding, $patched);
        if ($patched === $source) {
            continue;
        }
        if (file_put_contents($path, $patched) === false) {
            throw new RuntimeException('Unable to patch framework test: ' . $path);
        }
        $changed[] = $file;
    }
    return $changed;
};
