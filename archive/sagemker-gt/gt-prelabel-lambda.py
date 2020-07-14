import json


def lambda_handler(event, context):
    print('## ENVIRONMENT VARIABLES')
    # data = event['dataObject']
    # taskInput = {
    #     'taskObject': data['src-ref'],
    #     'labels': data

    # }
    return {
        "taskInput": event['dataObject']
    }