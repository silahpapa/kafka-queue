<?php

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config): KafkaQueue
    {
        $conf = new Conf([
            'bootstrap.servers' => $config['bootstrap_servers'],
            'security.protocol' => $config['security_protocol'],
            'sasl.mechanisms' => $config['sasl_mechanisms'],
            'sasl.username' => $config['sasl_username'],
            'sasl.password' => $config['sasl_password'],
        ]);

        $producer = new Producer($conf);

        $conf->set('group.id', $config['group_id']);
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new KafkaConsumer($conf);

        return new KafkaQueue($producer, $consumer);
    }
}
