<?php

$frameworkTests = dirname(__DIR__) . '/vendor/silverstripe/framework/tests';
$patchFrameworkTests = require __DIR__ . '/framework-test-compat.php';
$patchFrameworkTests($frameworkTests . '/php');

require $frameworkTests . '/bootstrap.php';
