# SQLite3 Module

[![CI](https://github.com/silverstripe/silverstripe-sqlite3/actions/workflows/ci.yml/badge.svg)](https://github.com/silverstripe/silverstripe-sqlite3/actions/workflows/ci.yml)

## Maintainer Contact

Andreas Piening (Nickname: apiening)
<andreas (at) silverstripe (dot) com>

## Installation

```sh
composer require silverstripe/sqlite3
```

## Configuration

Either use the installer to automatically install SQLite or add this to your _config.php (right after
"require_once("conf/ConfigureFromEnv.php");" if you are using _ss_environment.php)

```php
$databaseConfig['type'] = 'SQLite3Database';
$databaseConfig['path'] = "/path/to/my/database/file";
```

Make sure the webserver has sufficient privileges to write to that folder and that it is protected from
external access.

### Sample mysite/_config.php

```php
<?php
global $project;
$project = 'mysite';

global $database;
$database = 'SS_mysite';

require_once("conf/ConfigureFromEnv.php");

global $databaseConfig;
$databaseConfig = array(
	"type" => 'SQLite3Database',
	"server" => 'none',
	"username" => 'none',
	"password" => 'none',
	"database" => $database,
	"path" => "/path/to/my/database/file",
);
```

Again: make sure that the webserver has permission to read and write to the above path (/path/to/my/database/,
'file' would be the name of the sqlite db file)

## URL parameter

If you're trying to change a field constrain to NOT NULL on a field that contains NULLs dev/build fails because
it might corrupt existing records. In order to perform the action anyway add the URL parameter 'avoidConflict' when
running dev/build which temporarily adds a conflict clause to the field spec.
E.g.: http://www.my-project.com/?avoidConflict=1

## MySQL Compatibility

Enable optional MySQL-to-SQLite transpilation for custom queries:

```yaml
SilverStripe\SQLite\SQLite3SQLTranspiler:
  enable_mysql_compat: true
```

Converts MySQL functions to SQLite equivalents:

| MySQL | SQLite |
|-------|--------|
| `STRAIGHT_JOIN` | `JOIN` |
| `ON DUPLICATE KEY UPDATE` | `ON CONFLICT(...) DO UPDATE SET` |
| `NOW()` | `datetime('now')` |
| `UNIX_TIMESTAMP()` | `strftime('%s', 'now')` |
| `UNIX_TIMESTAMP(date)` | `strftime('%s', date)` |

## Open Issues

- Third-party modules with MySQL-specific SQL may need `enable_mysql_compat` enabled (see above)
- No fulltext search; built-in search doesn't order by relevance. Check out fts3
