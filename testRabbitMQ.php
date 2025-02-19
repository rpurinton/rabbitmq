<?php

require __DIR__ . '/vendor/autoload.php';

use RPurinton\RabbitMQ;
use React\EventLoop\{Loop, LoopInterface};

RabbitMQ::publish('rabbitmq-test', 'Hello, world!');

$loop = Loop::get();
RabbitMQ::connect('rabbitmq-test', function (string $message) use ($loop) : bool {
    if ($message === 'Hello, world!') {
        echo "Success!\n";
    } else {
        echo "Failure?!\nExpected 'Hello, world!', got '$message'\n";
    }
    echo ("Press CTRL+C to exit.\n");
    return true;
});
