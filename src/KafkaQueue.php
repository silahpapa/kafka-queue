<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;
use RdKafka\Message;
class KafkaQueue extends Queue implements QueueContract
{
    public function __construct(
        protected Producer $producer,
        protected KafkaConsumer $consumer
    ) {}

    public function size($queue = null)
    {
        return 0; // Implement the size method as needed
    }

    public function push($job, $data = '', $queue = null)
    {
        $topicName = $queue ?? env('KAFKA_QUEUE');
        $topic = $this->producer->newTopic($topicName);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(1000);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // Implement the pushRaw method as needed
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        // Implement the later method as needed
    }

    public function pop($queue = null): ?array
    {
        $this->consumer->subscribe([$queue]);
        $message = $this->consumer->consume(120 * 1000);
        match ($message->err) {
            RD_KAFKA_RESP_ERR_NO_ERROR => unserialize($message->payload)->handle(),
            RD_KAFKA_RESP_ERR__PARTITION_EOF => var_dump("No more messages; will wait for more\n"),
            RD_KAFKA_RESP_ERR__TIMED_OUT => var_dump("Timed out\n"),
            default => throw new \Exception($message->errstr(), $message->err)
        };
        $this->consumer->close();
        return null;
    }
}
