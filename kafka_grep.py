from flask import Flask, render_template, request, jsonify

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
    return jsonify(producer_bootstrap_server=producer_bootstrap_server, producer_kafka_topic=producer_kafka_topic,
                   producer_message=producer_message)


@app.route('/consume', methods=['GET', 'POST'])
def consume():
    consumer_bootstrap_server = request.form['consumer_bootstrap_server']
    consumer_kafka_topic = request.form['consumer_kafka_topic']
    consumer_offset = request.form['consumer_offset']
    consumer_timeout = request.form['consumer_timeout']
    return jsonify(consumer_bootstrap_server=consumer_bootstrap_server, consumer_kafka_topic=consumer_kafka_topic,
                   consumer_offset=consumer_offset, consumer_timeout=consumer_timeout)

if __name__ == "__main__":
    app.secret_key = 'Z62dpK8awX'
    app.run(debug=True)
