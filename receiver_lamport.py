#!/usr/bin/env python 
#coding: utf8  


#on précise l'encodage des caractéres en utf 8 ( l'implementation la plus utilisé de utf)


#Queue est utilisé pour stocker les requests elle permet d'ordonner les requests selon le time stamp
import Queue
import pika
import time
import sys

# les variables globales du programme 

req_list = Queue.PriorityQueue() #liste des requetes automatiquement ordonnée selon le timestamp

clock=0 # horloge logique de lamport  
num_workers = 1 # nombre de consommateurs 
num_ack = 0     # nombre de réponse
queue_id = ''   # identifiant de la file d'attente ( et du worker)



num_workers=sys.argv[1]  # on recupere en ligne de commande la valeur


         #ici on gére la partie horloge logique

#fonction pour la mise a jour de l'horloge de lamport
def update_clock(clock , timestamp):
	clock = max(clock, timestamp)

#fonction d'incrementation de l'horloge 
def increment_clock():
    clock += 1

    	#ici on gére l'envoie et la construction de message
	#on utilise 3 types de messages : Request , Liberation et réponse     

#fonction d'envoi des requests
def send_request():
    
    increment_clock()  # on incrémente l'horloge
    msg_type = 'Request' # le corps de notre message
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='Broadcast_msg', type='fanout') #broadcast
    result = channel.queue_declare(exclusive=True) # on supprime le channel a la deconnexion
    queue_id = result.method.queue #on récupére le nom de la file
    channel.queue_bind(exchange='Broadcast_msg', queue=queue_id) # on bind la file a l'exchange
 		
		#la bibliothéque pika nous permet de modifier les propriétés reply_to et 			timestamp automatiquement avec BasicProperties   
		

    options = pika.BasicProperties(reply_to=queue_id , timestamp=clock)
    channel.basic_publish(exchange='Broadcast_msg',routing_key='',body=msg_type, properties=options)
    
		# on ne met que options dans la liste des requetes parceque on a plus besoin du 		body du message ( si on le met dans req_list on sait forcement que c'est un 			message request) 

    req_list.put(options, False) 
    num_ack +=1 #le worker qui envoie le request s'autorise lui meme a entrer en zone critique 


#fonction pour envoyer des réponses cette fonction a comme parametre le id du worker et le timestamp du message reçu ( request)

def send_respond(queue_reply, timestamp):
	
    update_clock(clock, timestamp) # mise a jour de l'horloge
    msg_type = 'Respond'
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    
    options = pika.BasicProperties(reply_to=queue_id , timestamp=clock)
    channel.basic_publish(exchange='',routing_key=queue_reply ,body=msg_type, properties=options)
#on ne désire envoyer de réponse qu'au worker qui a envoyé le request d'ou routing_key



#fonction pour envoyer des messages release

def send_release():
    
    msg_type = 'Release'
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='Broadcast_msg', type='fanout')
    result = channel.queue_declare(exclusive=True)
    queue_id = result.method.queue 
    channel.queue_bind(exchange='Broadcast_msg', queue=queue_id)
    options = pika.BasicProperties(reply_to=queue_id , timestamp=clock)
    channel.basic_publish(exchange='Broadcast_msg',routing_key='',body=msg_type, properties=options)
    
    req_list.get(False) # on enleve le premier element de la liste



#ici on va traiter les messages qu'on reçoit pour appliquer l'algorithme de lamport



def callback(ch, method, options, body):

    print(" Received [x] %r" % body)
    
#on verifie le type du message
# le traitement pour request et release localement est deja fait au moment de l'envoie
    
#request

    if ( body == 'Request' and options.reply_to != queue_id): # on verifie le sender 
        update_clock(clock,options.timestamp)    
        req_list.put(options, False)
        send_respond(options.reply_to,clock)

#respond    
    elif ( body == 'Respond'):
        num_ack +=1
#release    

    elif ( body == 'Release' and options.reply_to !=queue_id): # on verifie le sender
        update_clock(clock, options.timestamp)
        req_list.get(False)
    else:
        print(' error !!! ')  # erreur


#fonction pour simuler un etat critique , on utilise un sleep
def simulate_critical():
    sleep(5000)


#cette fonction gére l'entrée dans la section critique
def enter_critical_section():    
    temp=req_list.get(False) # on retire le premier element
#on verifie que worker peut entrer en etat critique
    if(queue_id == temp.reply_to and num_ack == num_workers): 
            simulate_critical()
	    after_critical()        
    else:  
        req_list.put(temp,False) # sinon on remet l'element dans la liste
	

# on définie une fonction qui gére la liste aprés la sortie de l'etat critique d'un worker

def after_critical():
	send_release() # on envoie un release
	#si la liste est vide on a rien a traiter
	if list_req.empty():
	    return
	#sinon on récupére le premier element dans la liste
	request = list_req.get(False)
	# on vérifie qui est le worker concerné sachant que request c'est options
	
	if (request.reply_to != queue_id):
		send_respond(request.reply_to,clock)
	#on remet l'element retiré dans la liste
		list_req.put(request)
	else:
	# ça veux dire que le worker actuel est en tete de liste
		enter_critical_section();




	
	


















    
