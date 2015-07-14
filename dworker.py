#!/usr/bin/env python
import pika
import jsonpickle
import json
import sys
import os
from ansible.parsing import DataLoader
from ansible.errors import AnsibleError, AnsibleConnectionFailure
from ansible.executor.task_executor import TaskExecutor
from ansible.executor.task_result import TaskResult
from ansible.playbook.handler import Handler
from ansible.playbook.task import Task
from ansible.plugins.strategies import SharedPluginLoaderObj

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))

channel = connection.channel()

channel.queue_declare(queue='rpc')

def process_task(body):
    new_stdin = sys.stdin
    try:
        fileno = sys.stdin.fileno()
        if fileno is not None:
            try:
                new_stdin = os.fdopen(os.dup(fileno))
            except OSError, e:
                # couldn't dupe stdin, most likely because it's
                # not a valid file descriptor, so we just rely on
                # using the one that was passed in
                pass
    except ValueError:
        # couldn't get stdin's fileno, so we just carry on
        pass
    shared_loader_obj = SharedPluginLoaderObj()
    loader = DataLoader(vault_password=None)
    task_json = json.loads(body)
    loader.set_basedir(task_json['base_dir'])
    host = jsonpickle.decode(task_json['host'])
    task = jsonpickle.decode(task_json['task'])
    job_vars = jsonpickle.decode(task_json['task_vars'])
    connection_info  = jsonpickle.decode(task_json['conn_info'])
    task.set_loader(loader)
    new_connection_info = connection_info.set_task_and_host_override(task=task, host=host)
    executor_result = TaskExecutor(host, task, job_vars, new_connection_info, new_stdin, loader, shared_loader_obj).run()
    task_result = TaskResult(host, task, executor_result)
    task_pickled = jsonpickle.encode(task_result)
    return(json.dumps(task_pickled))
    

def on_request(ch, method, props, body):
    print "got request"
    response = process_task(body)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                     props.correlation_id),
                     body=response)
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_consume(on_request, queue='rpc')

print " [x] Awaiting RPC requests"
channel.start_consuming()
