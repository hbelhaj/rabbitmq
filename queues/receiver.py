#!/usr/bin/env python 
#coding: utf8
import pika
import time


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.queue_declare(queue='task2_queue',durable=True)

channel.basic_qos(prefetch_count=1) # not to give more than one message to a worker at a time 

def callback(ch , method , properties , body):
	print ( " [x] Received  %r " % body)
	time.sleep(body.count(b'.'))
	print ( " it's  done " )
	ch.basic_ack = method.delivery_tag)

channel.basic_consume(callback, queue= 'task2_queue')

print ( ' [*] waiting for messages . To exit press CTRL+C')

channel.start_consuming()
