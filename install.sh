#!/bin/bash

sudo mkdir /usr/local/bin/datacollector
sudo cp datacollector.py /usr/local/bin/datacollector/datacollector.py
sudo cp myconfig.json /usr/local/bin/datacollector/myconfig.json
sudo cp datacollector.service /lib/systemd/system/datacollector.service
sudo chmod 644 /lib/systemd/system/datacollector.service
sudo systemctl daemon-reload 
sudo systemctl enable datacollector.service

#reboot and systemctl status rabbitmq-objstore.service

