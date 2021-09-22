#
#
# Author: Aniruddha Gokhale
# CS4287-5287: Principles of Cloud Computing, Vanderbilt University
#
# Created: Sept 6, 2020
#
# Purpose:
#
#    Demonstrate the use of Kafka Python streaming APIs.
#    In this example, we use the "top" command and use it as producer of events for
#    Kafka. The consumer can be another Python program that reads and dumps the
#    information into a database OR just keeps displaying the incoming events on the
#    command line consumer (or consumers)
#

# IMPORTS
import os  # need this for popen
import datetime
import time  # for sleep
import json
import sys
from kafka import KafkaProducer  # producer of events
import config

"""kafka-python docs: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html"""
import requests


# GLOBAL FUNCTIONS
def get_api_key():
    """
    Get the API key from the environment variable
    """
    return os.environ.get("WEATHER_API_KEY")


def weather_request(city: str, api_key: str):
    """
    Make a request to the weather API and return the response
    LIMIT 60 calls per minute
    """
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=imperial"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
        # return response.content
    else:
        return None


def main():
    # acquire the producer
    producer = KafkaProducer(bootstrap_servers=config.kafka_servers, api_version=(2,13,0),
    acks=1)

     city = "New York"

    # wait for leader to write to log
    # if sys.argv[1] == "ny":
    #     city = "New York"
    #     topic_ = "ny"
    # elif sys.argv[1] == "chi":
    #     city = "Chicago"
    #     topic_ = "chi"
    # else:
    #     city = sys.argv[1]
    #     topic_ = sys.argv[1]

    # say we send the contents 100 times after a sleep of 1 sec in between
    api_key = get_api_key()
    for i in range(100):

        output = weather_request(city, api_key)


        if output != None:
            # send the output to the Kafka topic
            message = {"City": output['name'],
                       "Description": output['weather'][0]['description'],
                       "Temperature": output['main']['temp'],
                       "ts": datetime.datetime.now().isoformat()}
            print(message)
            # steralize data
            message = bytes(json.dumps(message), 'ascii')
            producer.send(topic=topic_, value=message)
            producer.flush()  # try to empty the sending buffer
            # sleep a second
            time.sleep(5)  # changed to 5 seconds for api limit
        else:
            raise Exception("Error in send")

    # we are done
    producer.close()


if __name__ == "__main__":
    main()
    