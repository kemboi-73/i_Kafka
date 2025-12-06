from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def send_notification(topic, payload):
    p.produce(topic, payload.encode('utf-8'), callback=delivery_report)
    p.poll(0)

    print(f'Message sent to {topic}: {payload}')

# Usage example
email_payload = '{"to": "receiver@example.com", "from": "sender@example.com", "subject": "Sample Email", "body": "This is a sample email notification"}'
send_notification('email-topic', email_payload)

sms_payload = '{"phoneNumber": "1234567890", "message": "This is a sample SMS notification"}'
send_notification('sms-topic', sms_payload)

p.flush()
