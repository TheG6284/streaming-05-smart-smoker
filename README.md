# streaming-05-smart-smoker


## The producer
So currently I have the program set up to require input a file name for the .csv.

Upon entering the name you will then see the periodic updates given by the console as each of the messages push to their queues.
Three separate queues currently exist, one for each of the temperatures we are tracking.
You can see an example of this in the screenshot bellow which has a console currently pushing messages as well as the RabbitMQ Admin Page.