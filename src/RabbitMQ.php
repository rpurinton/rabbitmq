<?php

declare(strict_types=1);

namespace RPurinton;

use RPurinton\Bunny\Async\Client as AsyncClient;
use RPurinton\Bunny\{Channel, Client, Message};
use React\EventLoop\{Loop, LoopInterface};
use React\Promise\Promise;
use Exception;

/**
 * Class RabbitMQ
 *
 * Handles asynchronous RabbitMQ message consumption and synchronous publishing.
 */
class RabbitMQ
{
    /**
     * RabbitMQ constructor.
     *
     * @param string   $queue    The queue to consume from.
     * @param callable $callback The callback to process messages.
     * @param LoopInterface|null $loop Optional event loop. If null, Loop::get() is used.
     *
     * @throws Exception If connection or consumption setup fails.
     */
    public function __construct(
        private string $queue,
        private $callback,
        private ?LoopInterface $loop = null
    ) {
        if (!$this->loop) {
            $this->loop = Loop::get();
        }
        $client = new AsyncClient($this->loop, self::config());
        $client->connect()
            ->then($this->getChannel(...))
            ->then($this->consume(...))
            ->catch(function (\Throwable $e): void {
                throw new Exception("RabbitMQ connection/error: " . $e->getMessage());
            });
    }

    /**
     * Obtains a channel from an asynchronous client.
     *
     * @param AsyncClient $client The asynchronous client instance.
     *
     * @return Promise The channel promise.
     *
     * @throws Exception If channel creation fails.
     */
    private function getChannel(AsyncClient $client): Promise
    {
        try {
            return $client->channel();
        } catch (\Throwable $e) {
            throw new Exception("Failed to create channel: " . $e->getMessage());
        }
    }

    /**
     * Sets up the channel for message consumption.
     *
     * @param Channel $channel The channel to consume messages on.
     *
     * @return void
     *
     * @throws Exception If setting up consumption fails.
     */
    private function consume(Channel $channel): void
    {
        try {
            $channel->qos(0, 1);
            $channel->consume($this->process(...), $this->queue);
        } catch (\Throwable $e) {
            throw new Exception("Failed to setup consumption: " . $e->getMessage());
        }
    }

    /**
     * Processes a single message.
     *
     * Attempts to decode JSON message content and passes data to the callback.
     * Acknowledges the message on success.
     *
     * @param Message $message The RabbitMQ message.
     * @param Channel $channel The channel instance.
     *
     * @return void
     *
     * @throws Exception If message processing encounters an error.
     */
    private function process(Message $message, Channel $channel): void
    {
        // Pass the raw content to the callback.
        ($this->callback)($message->content);
        $channel->ack($message);
    }

    /**
     * Publishes a message to the specified queue.
     *
     * Encodes the data as JSON, declares the queue, and publishes the message.
     * Ensures proper cleanup of the channel and client regardless of success or failure.
     * Returns true if the message was published successfully.
     *
     * @param string $queue The queue to publish to.
     * @param string $data  The data to publish.
     *
     * @return bool True if publishing is successful.
     *
     * @throws Exception If an error occurs during publishing or cleanup.
     */
    public static function publish(string $queue, string $data): bool
    {
        $client = null;
        $channel = null;
        try {
            $client = new Client(self::config());
            $client->connect();
            $channel = $client->channel();
            $channel->queueDeclare($queue, false, true, false, false);
            $channel->publish($data, [], '', $queue);
            return true;
        } catch (\Throwable $e) {
            throw new Exception("Error publishing message: " . $e->getMessage());
        } finally {
            if ($channel) {
                try {
                    $channel->close();
                } catch (\Throwable $e) {
                    throw new Exception("Error closing channel: " . $e->getMessage());
                }
            }
            if ($client) {
                try {
                    $client->disconnect();
                } catch (\Throwable $e) {
                    throw new Exception("Error disconnecting client: " . $e->getMessage());
                }
            }
        }
    }

    /**
     * Returns the RabbitMQ configuration.
     *
     * @return array The RabbitMQ configuration.
     */
    private static function config(): array
    {
        return Config::get("RabbitMQ", [
            "host"      => "string",
            "vhost"     => "string",
            "user"      => "string",
            "password"  => "string",
            "port"      => "int",
            "heartbeat" => "int",
        ]);
    }
}
