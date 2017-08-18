from flask import Flask, render_template, request, jsonify
from confluent_kafka import Producer
from confluent_kafka import Consumer
from random import *

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/producer')
def producer():
    return render_template('producer.html')


@app.route('/consumer')
def consumer():
    return render_template('consumer.html')


@app.route('/produce', methods=['GET', 'POST'])
def produce():
    producer_bootstrap_server = request.form['producer_bootstrap_server']
    producer_kafka_topic = request.form['producer_kafka_topic']
    producer_message = request.form['producer_message']

    kafka = Kafka(bootstrap_servers=producer_bootstrap_server)
    kafka.produce(producer_kafka_topic, producer_message)

    return jsonify(producer_bootstrap_server=producer_bootstrap_server, producer_kafka_topic=producer_kafka_topic,
                   producer_message=producer_message)


@app.route('/consume', methods=['GET', 'POST'])
def consume():
    consumer_bootstrap_server = request.form['consumer_bootstrap_server']
    consumer_kafka_topic = request.form['consumer_kafka_topic']
    consumer_offset = request.form['consumer_offset']
    consumer_timeout = request.form['consumer_timeout']
    consumer_group_id = request.form['consumer_group_id']
    return jsonify(consumer_bootstrap_server=consumer_bootstrap_server, consumer_kafka_topic=consumer_kafka_topic,
                   consumer_group_id=consumer_group_id, consumer_offset=consumer_offset,
                   consumer_timeout=consumer_timeout)


class Kafka(object):
    def __init__(self, bootstrap_servers, consumer_offset="latest", group_id=str(random())):
        """
        Initialize the bootstrap server to produce on.
        """
        producer_conf = {'bootstrap.servers': bootstrap_servers,
                         'queue.buffering.max.messages': 500000}
        self.producer = Producer(producer_conf)

        consumer_conf = {'bootstrap.servers': bootstrap_servers,
                         'group.id': group_id,
                         'default.topic.config': {'auto.offset.reset': consumer_offset}}
        self.consumer = Consumer(consumer_conf)

    def produce(self, kafka_topic, message):
        """
        writes the message to the specified topic
        """
        self.producer.produce(kafka_topic, message.encode('utf-8'))
        self.producer.flush()


if __name__ == "__main__":
    app.secret_key = 'Z62dpK8awX'
    app.run(debug=True)
