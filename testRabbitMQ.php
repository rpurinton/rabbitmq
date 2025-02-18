<?php

require __DIR__ . '/vendor/autoload.php';

use RPurinton\RabbitMQ;

RabbitMQ::publish('rabbitmq-test', 'Hello, world!');

$mq = new RabbitMQ('rabbitmq-test', function (string $message): void {
    if ($message === 'Hello, world!') {
        echo "Success!\n";
        exit(0);
    } else {
        echo "Failure?!\n";
        echo "Expected 'Hello, world!', got '$message'\n";
        exit(1);
    }
});
