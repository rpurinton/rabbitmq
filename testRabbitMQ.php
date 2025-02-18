<?php

require __DIR__ . '/vendor/autoload.php';

use RPurinton\RabbitMQ;
use React\EventLoop\Loop;

RabbitMQ::publish('rabbitmq-test', 'Hello, world!');

$loop = Loop::get();
$mq = new RabbitMQ('rabbitmq-test', function (string $message) use ($loop): void {
    if ($message === 'Hello, world!') {
        echo "Success!\n";
        $loop->stop();
        exit(0);
    } else {
        echo "Failure?!\n";
        echo "Expected 'Hello, world!', got '$message'\n";
        $loop->stop();
        exit(1);
    }
}, $loop);
