import time
import gevent
from gevent import monkey
from flask import Flask, render_template, request, jsonify
from confluent_kafka import Producer, Consumer
from flask_socketio import SocketIO, disconnect
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
    produce_kafka(producer_bootstrap_server, producer_kafka_topic, producer_message)

    return jsonify(producer_bootstrap_server=producer_bootstrap_server, producer_kafka_topic=producer_kafka_topic,
                   producer_message=producer_message)


@socketio.on('consumer_request', namespace='/consumerSocket')
def consumer_request(message):
    try:
        print "Got socket request"
        consumer_bootstrap_server = message['consumer_bootstrap_server']
        consumer_kafka_topic = message['consumer_kafka_topic']
        consumer_response_event = message['consumer_response_event']
        consumer_disconnect_event = message['consumer_disconnect_event']
        consumer_offset = "latest"
        if 'consumer_offset' in message:
            consumer_offset = message['consumer_offset']
        consumer_group_id = time.time()
        if 'consumer_group_id' in message:
            consumer_group_id = message['consumer_group_id']

        consume_kafka_and_emit_to_web_socket(consumer_bootstrap_server, consumer_kafka_topic,
                                             consumer_offset, consumer_group_id,
                                             response_event_name=consumer_response_event,
                                             disconnect_event_name=consumer_disconnect_event,
                                             namespace='/consumerSocket')
    except:
        traceback.print_exc()
    finally:
        print "End of socket response"


@socketio.on('disconnect_request', namespace='/consumerSocket')
def disconnect_request():
    print "Disconnect request"
    disconnect()


@socketio.on('disconnect', namespace='/consumerSocket')
def consumer_disconnect():
    print "disconnected"
    global connection_flag
    connection_flag = False


def consume_kafka_and_emit_to_web_socket(bootstrap_servers, kafka_topic, consumer_offset,
                                         consumer_group_id, response_event_name,
                                         disconnect_event_name, namespace):
    """
    Consumes from the provided topic
    and emits to provided namespace
    """
    global connection_flag
    connection_flag = True
    try:
        consumer_conf = {'bootstrap.servers': bootstrap_servers,
                         'group.id': consumer_group_id,
                         'default.topic.config': {'auto.offset.reset': consumer_offset}}
        c = Consumer(consumer_conf)
        topic_list = list()
        topic_list.append(kafka_topic)
        c.subscribe(topic_list)

        consumer_response = dict()
        consumer_response['consumer_bootstrap_server'] = bootstrap_servers
        consumer_response['consumer_kafka_topic'] = kafka_topic
        consumer_response['consumer_group_id'] = consumer_group_id
        consumer_response['consumer_offset'] = consumer_offset

        end_time = time.time() + 60
        print "Starting consumption"
        while time.time() < end_time and connection_flag:
            while True:
                msg = c.poll(timeout=3.0)
                if msg and not msg.error():
                    end_time = time.time() + 60

                    output = dict()
                    output[msg.offset()] = msg.value()
                    consumer_response['kafka_output'] = output

                    socketio.emit(response_event_name, {'consumer_response': consumer_response}, namespace=namespace)
                    gevent.sleep(0)
                    gevent.sleep(0)
                    print "Consumed:" + str(msg.value())
                else:
                    break
            time.sleep(2)
    except:
        traceback.print_exc()
    finally:
        print "End of consumption"
        socketio.emit(disconnect_event_name, namespace=namespace)
        connection_flag = False
        c.close()


def produce_kafka(bootstrap_servers, kafka_topic, message):
    """
        writes the message to the specified topic
    """
    producer_conf = {'bootstrap.servers': bootstrap_servers,
                     'queue.buffering.max.messages': 500000}
    try:
        p = Producer(producer_conf)
        p.produce(kafka_topic, message.encode('utf-8'))
        p.flush()
        print "Produced: " + str(message)
        return True
    finally:
        return False


if __name__ == "__main__":
    socketio.run(app=app, host="0.0.0.0")
