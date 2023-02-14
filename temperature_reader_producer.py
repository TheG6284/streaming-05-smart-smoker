"""
This program will read the smoker temperatures and push the information to Rabbit MQ Server

Gabb Albrecht
02/14/2023

"""

#importing pika for Rabbit MQ, csv to handle reading file, time to aid calculations, and sys
import pika
import sys
import csv
import time



def send_message(host: str, queue_name: str, message: str):
    try: 
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))

        ch = conn.channel()

        ch.queue_declare(queue=queue_name)

        ch.basic_publish(exchange ="", routing_key = queue_name, body = message)

        print(f"[x] Sent {message}")

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")

    finally:
        conn.close()

def get_grillin(file_name: str):
    input_file = open(file_name, "r")
    reader = csv.reader(input_file, delimiter = ",")

    next(reader)

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

def message_joiner(my_tuple: tuple):
    new_string = ",".join(my_tuple)
    return new_string

if __name__ == "__main__":
    
    file_name = input()
    
    get_grillin(file_name)
