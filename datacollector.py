#!/usr/bin/env python
__author__ = 'Riccardo Pozza, r.pozza@surrey.ac.uk'

import logging
import logging.handlers
import argparse
import sys
import pika
import json
import requests
import time
import datetime


def main():
    # Defaults
    LOG_FILENAME = "/var/log/datacollector.log"
    LOG_LEVEL = logging.INFO  # i.e. could be also "DEBUG" or "WARNING"

    # Define and parse command line arguments
    parser = argparse.ArgumentParser(description="Service for Starting Data Collection and Sync Time on Registration")
    parser.add_argument("-l", "--log", help="file to write log to (default '" + LOG_FILENAME + "')")

    # If the log file is specified on the command line then override the default
    args = parser.parse_args()
    if args.log:
        LOG_FILENAME = args.log

    # Configure logging to log to a file, making a new file at midnight and keeping the last 3 day's data
    # Give the logger a unique name (good practice)
    logger = logging.getLogger(__name__)
    # Set the log level to LOG_LEVEL
    logger.setLevel(LOG_LEVEL)
    # Make a handler that writes to a file, making a new file at midnight and keeping 3 backups
    handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when="midnight", backupCount=3)
    # Format each log message like this
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    # Attach the formatter to the handler
    handler.setFormatter(formatter)
    # Attach the handler to the logger
    logger.addHandler(handler)

    # Replace stdout with logging to file at INFO level
    sys.stdout = MyLogger(logger, logging.INFO)
    # Replace stderr with logging to file at ERROR level
    sys.stderr = MyLogger(logger, logging.ERROR)

    # RabbitMQ Credentials and Connection to Broker
    with open('/usr/local/bin/datacollector/myconfig.json') as config_file:
        config_data = json.load(config_file)

    # RabbitMQ Credentials and Connection to Broker
    print "Connection to RabbitMQ Message Broker"
    usercred = pika.PlainCredentials(config_data["RabbitUsername"], config_data["RabbitPassword"])
    connection = pika.BlockingConnection(pika.ConnectionParameters(config_data["RabbitHost"],config_data["RabbitPort"],'/',usercred))
    channel = connection.channel()
    print "Connected to RabbitMQ Message Broker"

    # need to declare exchange also in subscriber
    channel.exchange_declare(exchange='json_reg', type='fanout')

    print "Creating a temporary Message Queue"
    # temporary queue name, closed when disconnected
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    # bind to the queue considered attached to regs exchange
    channel.queue_bind(exchange='json_reg', queue=queue_name)
    print "Bound to the temporary Message Queue"

    _urllwm2mserver = "http://" + config_data["LWM2MServer"] + ":" + str(config_data["LWM2MServerPort"]) + "/api/clients/"
    print "Waiting for messages to Consume!"
    channel.basic_consume(lambda ch, method, properties, body:callback_ext_arguments(
                                 ch, method, properties, body,urllwm2mserver=_urllwm2mserver), queue=queue_name, no_ack=True)
    channel.start_consuming()

def callback_ext_arguments(ch, method, properties, body, urllwm2mserver):
    #body contains the json doc
    try:
        json_parsed = json.loads(body)
        client_just_registered = str(json_parsed['endpoint'])
        ret = sync_time(urllwm2mserver,client_just_registered)
        print str(json.loads(ret.content)['status']) + ", synchronized client: " + str(client_just_registered)
        ret = start_observation(urllwm2mserver, client_just_registered, 3301)
        print str(client_just_registered) + ", 3301, " + str(json.loads(ret.content)['status'])
        ret = start_observation(urllwm2mserver, client_just_registered, 3303)
        print str(client_just_registered) + ", 3303, " + str(json.loads(ret.content)['status'])
        ret = start_observation(urllwm2mserver, client_just_registered, 3304)
        print str(client_just_registered) + ", 3304, " + str(json.loads(ret.content)['status'])
        ret = start_observation(urllwm2mserver, client_just_registered, 3324)
        print str(client_just_registered) + ", 3324, " + str(json.loads(ret.content)['status'])
        ret = start_observation(urllwm2mserver, client_just_registered, 3325)
        print str(client_just_registered) + ", 3325, " + str(json.loads(ret.content)['status'])
        ret = start_observation(urllwm2mserver, client_just_registered, 3330)
        print str(client_just_registered) + ", 3330, " + str(json.loads(ret.content)['status'])
        ret = start_observation(urllwm2mserver, client_just_registered, 3348)
        print str(client_just_registered) + ", 3348, " + str(json.loads(ret.content)['status'])
    except:
        return

def sync_time(f_url, client):
    max_number_attempts = 5
    header_json = {'Content-Type': 'application/json'}
    parameters = {'format': 'Text'}
    objecturl = "/" + str(3) + "/" + str(0) + "/" + str(13) # time
    fullurl = f_url + client + objecturl
    attempts = 0
    while attempts < max_number_attempts:
        time_now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        payload_now = {'id': 13, 'value': time_now}
        r = requests.put(fullurl,headers=header_json, json=payload_now, params=parameters)
        attempts += 1
        if r.status_code == requests.codes.ok:
            if json.loads(r.content)['status'] == 'CHANGED':
                print "Synchronized with: " + str(time_now)
                return r
        time.sleep(attempts * 0.2)

    print "Maximum Number of Attempts achieved!"
    return r

def start_observation(f_url, client, object):
    max_number_attempts = 5
    objecturl = "/" + str(object) + "/" + str(0)
    fullurl = f_url + client + objecturl + "/observe"
    attempts = 0
    while attempts < max_number_attempts:
        time_now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        r = requests.post(fullurl)
        attempts += 1
        if r.status_code == requests.codes.ok:
            if json.loads(r.content)['status'] == 'CONTENT':
                print "Started Observation at: " + str(time_now) + " of " + client + objecturl
                return r
        time.sleep(attempts * 0.2)

    print "Maximum Number of Attempts achieved!"
    return r

# Make a class we can use to capture stdout and sterr in the log
class MyLogger(object):
        def __init__(self, logger, level):
                """Needs a logger and a logger level."""
                self.logger = logger
                self.level = level

        def write(self, message):
                # Only log if there is a message (not just a new line)
                if message.rstrip() != "":
                        self.logger.log(self.level, message.rstrip())

if __name__ == '__main__':
    main()
