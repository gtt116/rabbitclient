#!/usr/bin/env python
"""
A Kombu based RabbitMQ server client
"""
import sys
import argparse
import json
import pprint
try:
    from kombu.messaging import Producer
    from kombu import Exchange, Queue, Connection
except ImportError:
    print 'Please install kombu before running this script.'
    print 'You can run it on Nova compute.'
    sys.exit(1)


class RabbitClient(object):
    def __init__(self, host, username='guest', password='guest'):
        self.host = host
        self.username = username
        self.password = password
        self._amqp_connection = 'amqp://%s:%s@%s' % (self.username,
                                                     self.password,
                                                     self.host)
        self.conn = None

    def _channel(self):
        if not self.conn:
            self.conn = Connection(self._amqp_connection)

        return self.conn.channel()

    def queue_delete(self, queue_name):
        # NOTE(gtt): We can omit exchange and routing_key argument here
        # queue = Queue(queue_name, exchange=exchange,
        #      routing_key=routing_key, channel=conn.channel())
        queue = Queue(queue_name, channel=self._channel())
        return queue.delete()

    def queue_get(self, queue_name, ack=True):
        queue = Queue(queue_name, channel=self._channel())
        msg = queue.get()
        if not msg:
            return None
        if ack:
            msg.ack()
        return msg

    def queue_publish(self, routing_key,
                      exchange_name, exchange_type='topic', body=None):
        exchange = Exchange(name=exchange_name, type=exchange_type,
                            exclusive=False, durable=False, auto_delete=False)
        p = Producer(self._channel(), exchange, routing_key=routing_key)
        return p.publish(body)

    def queue_get_print(self, queue_name):
        msg = self.queue_get(queue_name)
        if not msg:
            print None
            return

        try:
            print json.dumps(json.loads(msg.body), indent=2)
        except ValueError:
            print msg.body

    def dispatch(self, action_name, args):
        if action_name == 'queue-get':
            return self.queue_get_print(args.queue_name)
        if action_name == 'queue-delete':
            return self.queue_delete(args.queue_name)

        raise ValueError("Method not support: %s" % action_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host')
    parser.add_argument('-u', '--username')
    parser.add_argument('-p', '--password')

    subparser = parser.add_subparsers(dest='action',
                                      help='commands help')

    delete_parser = subparser.add_parser('queue-delete')
    delete_parser.add_argument('queue_name')

    get_parser = subparser.add_parser('queue-get')
    get_parser.add_argument('queue_name')

    args = parser.parse_args()

    rabbit = RabbitClient(args.host, args.username, args.password)
    rabbit.dispatch(args.action, args)
