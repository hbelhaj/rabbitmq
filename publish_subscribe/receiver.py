#!/usr/bin/env python 
#coding: utf8
import pika
import time
import queue

#variables utilises dans le programme

 num_workers = 1 
 clock = 0
 num_ack = 0
 requests=queue.PriorityQueue() #automatiquement ordonné suivant le timestamp



def update_clock(clock , timestamp):
	clock=max(clock, timestamp)

#on envoie un message ou le body est type_mes  si le message est en broadcast
#on l'envoie a tout le monde sinon seulement au site a qui il faut repondre
def send_msg(type_mes , routing_key , 



connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(exchange='lamport',exchange_type='fanout')
result = channel.queue_declare(exclusive=True)
worker_id = result.method.queue
channel.queue_bind(exchange='lamport',queue=queue_num)



print ( " worker name  is  %s " , %worker_id)
def callback(ch , method , properties , body):
	print ( " [x]   %r:%r " %(method.routing_key,body))


channel.basic_consume(callback, queue=queue_name,no_ack=True)

channel.start_consuming()
