"""
This program will read the smoker temperatures and push the information to Rabbit MQ Server

Gabbs Albrecht
02/21/2023

"""

#imports listed at front of module
import pika
import sys
import csv
import time

#Defining Variables at head of the module
file_name = "smoker-temps.csv"


#A quick function to clear out the queues of any previous messges, decided defing in producer made the most sense due to restarting the producer would mean a new smoking session
def delete_queue(host: str, queue_name: str):
  
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    ch = conn.channel()
    ch.queue_delete(queue=queue_name)

#This is the function that defines the behavior to send messages
def send_message(host: str, queue_name: str, message: str):
    
    #Attempts to connect to the host and push the message to the defined queue
    try: 
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        ch = conn.channel()
        ch.queue_declare(queue=queue_name, durable = True)
        ch.basic_publish(exchange ="", routing_key = queue_name, body = message)
        print(f"[x] Sent {message}")
    
    #If the attempt fails, sends us this error message
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
    
        #Closes our connection to the host
    finally:
        conn.close()

#reads the temperatures and 
def get_grillin(file_name: str):
    
    #opens csv, reads it, and skips over header row
    input_file = open(file_name, "r")
    reader = csv.reader(input_file, delimiter = ",")
    next(reader)

    #resets the queues that are going to be used
    delete_queue("localhost","smoker")
    delete_queue("localhost","food_A")
    delete_queue("localhost","food_B")

    #for each row, splits the row into timestamp/temperature tupples and sends out each into correct queue
    for row in reader:
        timestamp, smoker_temp, foodA_temp, foodB_temp = row

        smoker_update = message_joiner((timestamp, smoker_temp))
        send_message("localhost","smoker",smoker_update)
        
        foodA_update = message_joiner((timestamp, foodA_temp))
        send_message("localhost","food_A",foodA_update)
        
        foodB_update = message_joiner((timestamp, foodB_temp))
        send_message("localhost","food_B",foodB_update)


        print("All 3 messages sent")
        time.sleep(30)

#defined the tuple joining process as a function to easily convert them into strings to send
def message_joiner(my_tuple: tuple):
    new_string = ",".join(my_tuple)
    return new_string

#Standard python idiom that let's us run our code as a script
if __name__ == "__main__":
    get_grillin(file_name)
