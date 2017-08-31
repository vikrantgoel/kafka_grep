import time
from flask import Flask, render_template, request, jsonify
from confluent_kafka import Producer
from confluent_kafka import Consumer
from random import *
from flask_socketio import SocketIO
from gevent import monkey
import traceback

monkey.patch_all()

app = Flask(__name__)
app.config['SECRET_KEY'] = 'Z62dpK8awX'
socketio = SocketIO(app)

connection_flag = False

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

    print "Starting production"
    kafka = Kafka(bootstrap_servers=producer_bootstrap_server)
    kafka.produce(producer_kafka_topic, producer_message)

    return jsonify(producer_bootstrap_server=producer_bootstrap_server, producer_kafka_topic=producer_kafka_topic,
                   producer_message=producer_message)


@app.route('/consume', methods=['GET', 'POST'])
def consume():
    consumer_bootstrap_server = request.form['consumer_bootstrap_server']
    consumer_kafka_topic = request.form['consumer_kafka_topic']

    consumer_offset = "earliest"
    if 'consumer_offset' in request.form:
        consumer_offset = request.form['consumer_offset']

    consumer_group_id = time.time()
    if 'consumer_group_id' in request.form:
        consumer_group_id = request.form['consumer_group_id']

    kafka = Kafka(bootstrap_servers=consumer_bootstrap_server, consumer_offset=consumer_offset,
                  group_id=consumer_group_id)
    print "Starting consumption"
    output = kafka.consume(kafka_topic=consumer_kafka_topic)
    print "Consumption output: " + str(output)

    return jsonify(consumer_bootstrap_server=consumer_bootstrap_server, consumer_kafka_topic=consumer_kafka_topic,
                   consumer_group_id=consumer_group_id, consumer_offset=consumer_offset,
                   consumer_messages=output)


@socketio.on('consumer_request', namespace='/consumerSocket')
def consumer_request(message):
    global connection_flag
    connection_flag = True

    consumer_bootstrap_server = message['consumer_bootstrap_server']
    consumer_kafka_topic = message['consumer_kafka_topic']
    consumer_offset = "earliest"
    if 'consumer_offset' in message:
        consumer_offset = message['consumer_offset']
    consumer_group_id = time.time()
    if 'consumer_group_id' in message:
        consumer_group_id = message['consumer_group_id']

    consumer_response = dict()
    consumer_response['consumer_bootstrap_server'] = consumer_bootstrap_server
    consumer_response['consumer_kafka_topic'] = consumer_kafka_topic
    consumer_response['consumer_group_id'] = consumer_group_id
    consumer_response['consumer_offset'] = consumer_offset

    end_time = time.time() + 3
    while time.time() < end_time and connection_flag:
        kafka = Kafka(bootstrap_servers=consumer_bootstrap_server, consumer_offset=consumer_offset,
                      group_id=consumer_group_id)
        print "Starting consumption"
        output = kafka.consume(kafka_topic=consumer_kafka_topic)
        print "Consumption output: " + str(output)

        if output:
            end_time = time.time() + 3
            consumer_response['kafka_output'] = output
            socketio.emit('consumer_response', {'consumer_response': consumer_response}, namespace="/consumerSocket")

        time.sleep(1)


@socketio.on('disconnect', namespace='/consumerSocket')
def consumer_disconnect():
    print "disconnected"
    global connection_flag
    connection_flag = False


class Kafka(object):
    def __init__(self, bootstrap_servers, consumer_offset="latest", group_id=str(random())):
        """
        Initialize the producer and consumer.
        """
        self.producer_conf = {'bootstrap.servers': bootstrap_servers,
                              'queue.buffering.max.messages': 500000}

        self.consumer_conf = {'bootstrap.servers': bootstrap_servers,
                              'group.id': group_id,
                              'default.topic.config': {'auto.offset.reset': consumer_offset}}

    def produce(self, kafka_topic, message):
        """
        writes the message to the specified topic
        """
        try:
            print "Producing: " + str(message)
            p = Producer(self.producer_conf)
            p.produce(kafka_topic, message.encode('utf-8'))
            p.flush()
            return True
        finally:
            return False

    def consume(self, kafka_topic):
        """
        Consumes from the provided topic for ttl_seconds
        and writes to standard output or the provided file
        """
        try:
            c = Consumer(self.consumer_conf)
            topic_list = list()
            topic_list.append(kafka_topic)
            c.subscribe(topic_list)

            output = dict()
            msg = c.poll(timeout=5.0)
            if msg and not msg.error():
                output[msg.offset()] = msg.value()
                c.commit(async=False)
            return output
        except:
            traceback.print_exc()
        finally:
            c.close()


if __name__ == "__main__":
    socketio.run(app=app, host="0.0.0.0")
