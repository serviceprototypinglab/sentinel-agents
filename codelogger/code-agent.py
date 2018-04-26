# -----------------------------------------------------------------------------
# Copyright (c) 2017. ZHAW - ICCLab
#  All Rights Reserved.
#
#     Licensed under the Apache License, Version 2.0 (the "License"); you may
#     not use this file except in compliance with the License. You may obtain
#     a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#     WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#     License for the specific language governing permissions and limitations
#     under the License.
#
#
#
#     Author: Piyush Harsh,
#     URL: piyush-harsh.info
# ------------------------------------------------------------------------------
import configparser
from kafka import KafkaProducer
import jsonpickle
from inspect import getframeinfo, stack
from time import sleep

config = configparser.RawConfigParser()
config.read("sentinel-agent.conf")


def get_section():
    return config.sections()


def get_elements(section_name):
    return config[section_name]


def get_element_value(section_name, element_name):
    return config[section_name][element_name]


def get_kafka_producer(endpoint, key_serializer, value_serializer):
    if key_serializer == "StringSerializer" and value_serializer == "StringSerializer":
        return KafkaProducer(linger_ms=1, acks='all', retries=0, key_serializer=str.encode,
                             value_serializer=str.encode, bootstrap_servers=[endpoint])


kafka_producer = get_kafka_producer(get_element_value("kafka-endpoint", "endpoint"),
                                    get_element_value("kafka-endpoint", "keySerializer"),
                                    get_element_value("kafka-endpoint", "valueSerializer"))


class SentinelLogger:
    @staticmethod
    def send_msg(msg):
        kafka_producer.send(get_element_value("sentinel", "topic"), key=get_element_value("sentinel", "seriesName"),
                            value=msg)

    def debug(self, message):
        msg_dict = {}
        msg_dict["agent"] = get_element_value("sentinel", "agent")
        msg_dict["level"] = "debug"
        caller = getframeinfo(stack()[1][0])
        msg_dict["method"] = caller.function + ":" + str(caller.lineno)
        msg_dict["file"] = caller.filename
        msg_dict["msg"] = message
        msg_to_send = jsonpickle.encode(msg_dict)
        SentinelLogger.send_msg(msg_to_send)

    def trace(self, message):
        msg_dict = {}
        msg_dict["agent"] = get_element_value("sentinel", "agent")
        msg_dict["level"] = "trace"
        caller = getframeinfo(stack()[1][0])
        msg_dict["method"] = caller.function + ":" + str(caller.lineno)
        msg_dict["file"] = caller.filename
        msg_dict["msg"] = message
        msg_to_send = jsonpickle.encode(msg_dict)
        SentinelLogger.send_msg(msg_to_send)

    def info(self, message):
        msg_dict = {}
        msg_dict["agent"] = get_element_value("sentinel", "agent")
        msg_dict["level"] = "info"
        caller = getframeinfo(stack()[1][0])
        msg_dict["file"] = caller.filename
        msg_dict["method"] = caller.function + ":" + str(caller.lineno)
        msg_dict["msg"] = message
        msg_to_send = jsonpickle.encode(msg_dict)
        SentinelLogger.send_msg(msg_to_send)

    def warn(self, message):
        msg_dict = {}
        msg_dict["agent"] = get_element_value("sentinel", "agent")
        msg_dict["level"] = "warn"
        caller = getframeinfo(stack()[1][0])
        msg_dict["method"] = caller.function + ":" + str(caller.lineno)
        msg_dict["file"] = caller.filename
        msg_dict["msg"] = message
        msg_to_send = jsonpickle.encode(msg_dict)
        SentinelLogger.send_msg(msg_to_send)

    def error(self, message):
        msg_dict = {}
        msg_dict["agent"] = get_element_value("sentinel", "agent")
        msg_dict["level"] = "error"
        caller = getframeinfo(stack()[1][0])
        msg_dict["method"] = caller.function + ":" + str(caller.lineno)
        msg_dict["file"] = caller.filename
        msg_dict["msg"] = message
        msg_to_send = jsonpickle.encode(msg_dict)
        SentinelLogger.send_msg(msg_to_send)


if __name__ == '__main__':
    logger = SentinelLogger()
    logger.warn("Unable to open mongodb endpoint")
    sleep(0.05)
    logger.error("user db save failed")
    sleep(0.05)
    logger.debug("retrying some other method")
    sleep(0.05)
    logger.info("this is an info method")
    sleep(0.05)
    logger.trace("this is a trace method")