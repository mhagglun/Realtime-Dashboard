#!/usr/bin/env python
'''
Producer for generating dummy access log data which is then written to a Kafka topic.
'''
import sys
import argparse
import time
import datetime
import numpy as np
import random
import logging
import threading
import json
from kafka import KafkaProducer
from faker import Faker
from tzlocal import get_localzone
local = get_localzone()


class Producer(threading.Thread):
    daemon = True

    def run(self, topic):
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: json.dumps(
                                     v).encode('utf-8'))

        while True:
            faker = Faker()

            timestamp = datetime.datetime.now()

            responseCodes = ["200", "404", "500", "403"]

            requestPaths = ["POST /auth/login", "GET /index.html", "GET /images/product?itemId=%s", "GET /store/search?name=%s",
                            "PUT /user", "POST /store/checkout?userId=%s&itemId=%s"]

            productNames = ["boots", "jacket", "beanie",
                            "pants", "dresses", "suits", "hoodies", "shirts"]
            productIds = random.sample(range(1000, 15000), 8)

            userAgents = [faker.firefox, faker.chrome, faker.safari,
                          faker.internet_explorer, faker.opera]

            ip = str(faker.ipv4())
            timestamp = str(timestamp.strftime('%d/%b/%Y:%H:%M:%S'))
            timeZone = str(datetime.datetime.now(local).strftime('%z'))

            path = str(np.random.choice(requestPaths, p=[
                       0.05, 0.25, 0.3, 0.3, 0.05, 0.05]))

            # Format request paths when necessary
            if "?userId=%s&itemId=%s" in path:
                path = path % (random.randint(1000, 9999),
                               np.random.choice(productIds))
            elif "?itemId=%s" in path:
                path = path % np.random.choice(productIds)
            elif "?name=%s" in path:
                path = path % np.random.choice(productNames)

            resp = np.random.choice(
                responseCodes, p=[0.9, 0.04, 0.02, 0.04])

            byteSize = str(int(random.gauss(5000, 50)))

            ua = str(np.random.choice(
                userAgents, p=[0.5, 0.3, 0.1, 0.05, 0.05])())

            message = '%s - - [%s %s] "%s %s HTTP/1.0" %s "%s"\n' % (
                ip, timestamp, timeZone, path, resp, byteSize, str(ua))

            producer.send(topic, message)
            print(message)

            # time between each send action
            time.sleep(random.randint(1, 5))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-topic", "--topic", help="The name of the kafka topic to write log events to")
    args = parser.parse_args()

    if args.topic is None:
        print("Error: Missing required flag: -topic, --topic.\nTry 'producer --help' for more information. ")
        sys.exit(1)

    producer = Producer()
    producer.run(args.topic)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
