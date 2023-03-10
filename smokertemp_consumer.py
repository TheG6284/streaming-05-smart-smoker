"""
This program will read the smoker temperatures and push the information to Rabbit MQ Server

Gabbs Albrecht
02/21/2023

"""

#imports listed at head of module
import pika
import sys
import time
from collections import deque

#Defining Variables at head of the module
host = "localhost"
queue = "smoker"
smokertemp_deque = deque(maxlen = 5)
smokertemp_alert = -15


#Callback function to handle the incoming messages
def callback(ch, method, properties, body):
    
    #Sends confirmation of message received
    message = body.decode()
    print(f" [x] Received {message}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

    #Attempts to split timestamp from temperature, and calculate the change over the time window
    try:
        smokertemp_current = float(message.split(",")[1])
        smokertemp_deque.append(smokertemp_current)
        smokertemp_change = smokertemp_current - smokertemp_deque[0]
        
        #Sends warning if temperature drastically drops
        if smokertemp_change < smokertemp_alert:
            print(f"TEMPERATURE DROP: Your smoker temperature decreased rapidly. Please check on it at once!")

    #Returns that no temperature value was read
    except Exception as e:
        smokertemp_deque.append(0.0)
        print("No temperature reading received")


#Continuously listens for messages on a queue, smoker queue is the default.
def main(hn: str = "localhost", qn: str = "smoker"):
    
    #Attempts to connect to the host
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    #Throws this eror message if the connection cannot be established
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    #Declares and connects to proper queue, defines our callback function for handling messages on it, and starts consuming messages
    try:
        channel = connection.channel()
        channel.queue_declare(queue=qn, durable = True)
        channel.basic_qos(prefetch_count=1) 
        channel.basic_consume( queue=qn, on_message_callback=callback)
        print(" [*] Ready for work. To exit press CTRL+C")
        channel.start_consuming()

    #Message thrown in case of error
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)

    #Allows user to interupt process
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)

    #Closes the connection
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()

#Standard python idiom that let's us run our code as a script
if __name__ == "__main__":
    main(host, queue)