#!/usr/bin/env php
<?php

require __DIR__ . '/vendor/autoload.php';

use RPurinton\RabbitMQ;
use React\EventLoop\Loop;

RabbitMQ::publish('rabbitmq-test', 'Hello, world!');

$loop = Loop::get();
RabbitMQ::connect('rabbitmq-test', function (string $message) use ($loop): bool {
    if ($message === 'Hello, world!') echo "Success!\n";
    else echo "Failure?!\nExpected 'Hello, world!', got '$message'\n";
    $loop->futureTick(function () use ($loop) { $loop->stop(); });
    return true;
}, $loop);
