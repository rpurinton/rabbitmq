<?php

declare(strict_types=1);

namespace RPurinton;

use RPurinton\{Config, Log};
use RPurinton\Bunny\Async\Client as AsyncClient;
use RPurinton\Bunny\{Channel, Client, Message};
use React\EventLoop\{Loop, LoopInterface};
use React\Promise\Promise;
use RPurinton\Exceptions\RabbitMQException;

/**
 * Class RabbitMQ
 * Handles asynchronous RabbitMQ message consumption and synchronous publishing.
 */
class RabbitMQ
{
    public $channel;
    /**
     * RabbitMQ constructor.
     * @param string   $queue    The queue to consume from.
     * @param callable $callback The callback to process messages.
     * @param LoopInterface|null $loop Optional event loop. If null, Loop::get() is used.
     * @param bool     $passive  Whether to passively declare the queue. Defaults to false.
     * @param bool     $durable  Whether the queue is durable. Defaults to false.
     * @param bool     $exclusive Whether the queue is exclusive. Defaults to false.
     * @param bool     $autoDelete Whether the queue is auto-deleted. Defaults to true.
     * @param bool     $noWait   Whether to wait for a reply. Defaults to false.
     * @param array    $arguments Additional arguments for queue declaration. Defaults to an empty array.
     * @param array    $rabbitMQConfig Optional RabbitMQ configuration. Defaults to using the Config class.
     */
    public function __construct(
        private string $queue,
        private $callback,
        private ?LoopInterface $loop = null,
        private bool $passive = false,
        private bool $durable = false,
        private bool $exclusive = false,
        private bool $autoDelete = true,
        private bool $noWait = false,
        private array $arguments = [],
        private array $rabbitMQConfig = []
    ) {}

    public function run()
    {
        Log::install();
        Log::trace("RabbitMQ::run()");
        if (!$this->loop) $this->loop = Loop::get();
        $client = new AsyncClient($this->loop, self::config($this->rabbitMQConfig));

        $client->connect()
            ->then($this->getChannel(...))
            ->then($this->consume(...))
            ->catch(function (\Throwable $e): void {
                throw new RabbitMQException("Error running RabbitMQ: " . $e->getMessage());
            });
        Log::debug("RabbitMQ::run() - running");
        return $this;
    }

    public static function connect($queue, $callback, $loop = null, $passive = false, $durable = false, $exclusive = false, $autoDelete = true, $noWait = false, $arguments = [], $rabbitMQConfig = [])
    {
        return (new self($queue, $callback, $loop, $passive, $durable, $exclusive, $autoDelete, $noWait, $arguments, $rabbitMQConfig))->run();
    }

    /**
     * Obtains a channel from an asynchronous client.
     * @param AsyncClient $client The asynchronous client instance.
     * @return Promise The channel promise.
     * @throws RabbitMQException If channel creation fails.
     */
    private function getChannel(AsyncClient $client): Promise
    {
        try {
            $this->channel = $client->channel();
            return $this->channel;
        } catch (\Throwable $e) {
            throw new RabbitMQException("Failed to create channel: " . $e->getMessage());
        }
    }

    /**
     * Sets up the channel for message consumption.
     *
     * @param Channel $channel The channel to consume messages on.
     *
     * @return void
     *
     * @throws RabbitMQException If setting up consumption fails.
     */
    private function consume(Channel $channel): void
    {
        try {
            $channel->queueDeclare($this->queue, $this->passive, $this->durable, $this->exclusive, $this->autoDelete, $this->noWait, $this->arguments);
            $channel->qos(0, 1);
            $channel->consume($this->process(...), $this->queue);
        } catch (\Throwable $e) {
            throw new RabbitMQException("Failed to setup consumption: " . $e->getMessage());
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
     * @throws RabbitMQException If message processing encounters an error.
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
     * @throws RabbitMQException If an error occurs during publishing or cleanup.
     */
    public static function publish(string $queue, string $data, bool $passive = false, bool $durable = false, bool $exclusive = false, bool $autoDelete = true, bool $noWait = false, array $arguments = [], array $rabbitMQConfig = []): bool
    {
        $client = null;
        $channel = null;
        try {
            $client = new Client(self::config($rabbitMQConfig));
            $client->connect();
            $channel = $client->channel();
            $channel->queueDeclare($queue, $passive, $durable, $exclusive, $autoDelete, $noWait, $arguments);
            $channel->publish($data, [], '', $queue);
            return true;
        } catch (\Throwable $e) {
            throw new RabbitMQException("Error publishing message: " . $e->getMessage());
        } finally {
            if ($channel) {
                try {
                    $channel->close();
                } catch (\Throwable $e) {
                    throw new RabbitMQException("Error closing channel: " . $e->getMessage());
                }
            }
            if ($client) {
                try {
                    $client->disconnect();
                } catch (\Throwable $e) {
                    throw new RabbitMQException("Error disconnecting client: " . $e->getMessage());
                }
            }
        }
    }

    /**
     * Returns the RabbitMQ configuration.
     *
     * @return array The RabbitMQ configuration.
     */
    private static function config(array $rabbitMQConfig = []): array
    {
        $rules = [
            "host"      => "string",
            "vhost"     => "string",
            "user"      => "string",
            "password"  => "string",
            "port"      => "integer",
            "heartbeat" => "integer",
        ];
        if (!empty($rabbitMQConfig)) {
            foreach ($rabbitMQConfig as $key => $value) {
                if (!array_key_exists($key, $rules)) {
                    throw new RabbitMQException("Invalid RabbitMQ configuration key: $key");
                }
                if (gettype($value) !== $rules[$key]) {
                    throw new RabbitMQException("Invalid RabbitMQ configuration value for $key: $value");
                }
            }
            return $rabbitMQConfig;
        }
        return Config::get("RabbitMQ", $rules);
    }
}
