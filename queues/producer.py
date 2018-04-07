#!/usr/bin/env python 
import pika
import sys


message  = ' '.join(sys.argv[1:]) or " hello world ! "

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

channel = connection.channel() 

channel.basic_qos(prefetch_count=1)

channel.queue_declare(queue ='task2_queue',durable=True)

channel.basic_publish(exchange='',routing_key='task2_queue', body=message, properties = pika.BasicProperties(delivery_mode = 2))

print ( " zebi sent %r " %message)

connection.close()
