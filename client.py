import json
import pprint
from kombu.messaging import Producer
from kombu import Exchange, Queue, Connection

exchange_name = 'neutron'
routing_key = 'neutron_notifications.info'
amqp_connection = 'amqp://guest:ntse@localhost'
conn = Connection(amqp_connection)


# get message
def get_mesage():
    exchange = Exchange(name=exchange_name, type='topic',
                        exclusive=False, durable=False, auto_delete=False)

    queue_name = 'neutron_notifications.info'
    q = Queue(queue_name, exchange=exchange,
              routing_key=routing_key, channel=conn.channel())
    msg = q.get()

    if msg is None:
        print 'No messages'
        return

    try:
        pprint.pprint(json.loads(msg.body), indent=2)
    except ValueError:
        print msg.body
    finally:
        msg.ack()


# publish message
def send_message(msg):

    exchange = Exchange(name=exchange_name, type='topic',
                        exclusive=False, durable=False, auto_delete=False)
    p = Producer(conn.channel(), exchange, routing_key=routing_key)
    p.publish(msg)


if __name__ == '__main__':
#    send_message('fuck')
    get_mesage()
