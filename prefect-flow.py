# prefect flow goes here
import requests
from prefect import task, flow, get_run_logger
import boto3, time
url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/vcx4ka"
sqs = boto3.client('sqs')


@task
def populate_queue():
    # populate the SQS queue by POSTing to the API endpoint
    logger = get_run_logger()
    payload = requests.post(url).json()
    logger.info(f"Queue populated. URL: {payload['sqs_url']}")
    return payload["sqs_url"]

@task
def monitor_queue(queue_url):
    # poll the queue attributes until all 21 messages are available
    logger = get_run_logger()
    waited=0
    while True:
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesDelayed",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )["Attributes"]

        # log the number of messages visible, delayed, and en route
        visible = int(attrs["ApproximateNumberOfMessages"])
        delayed = int(attrs["ApproximateNumberOfMessagesDelayed"])
        not_visible = int(attrs["ApproximateNumberOfMessagesNotVisible"])
        
        logger.info(f"Visible:{visible}, Delayed:{delayed}, InFlight:{not_visible}")

        # stop loop if all messages are in the queue
        if (visible+delayed+not_visible >= 21) and (delayed == 0):
            logger.info(f"All messages are now available in the queue. Waited {waited} total seconds for messages to arrive.")
            return True
        time.sleep(30)
        waited += 30

def get_message(queue_url):
    logger = get_run_logger()
    # try to get ONE message. return None if queue is empty.
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MessageSystemAttributeNames=['All'],
            MaxNumberOfMessages=1,
            VisibilityTimeout=60,
            MessageAttributeNames=['All'],
            WaitTimeSeconds=5
        )
        if 'Messages' not in response:
            logger.info("No messages in the queue.")
            return None
        
        # log information about the message if received
        logger.info(f"Message received successfully! Message details below...")
        logger.info(f"MessageAttributes: {response['Messages'][0]['MessageAttributes']}")
        logger.info(f"Order No: {response['Messages'][0]['MessageAttributes']['order_no']['StringValue']}")
        return response['Messages'][0]

    except Exception as e:
        print(f"Error getting message: {e}")
        raise e

def delete_message(queue_url, receipt_handle):
    # delete the message from the queue using its receipt handle
    logger = get_run_logger()
    try:
        response = sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        logger.info(f"Deleting message from queue. Response: \n{response}")
    except Exception as e:
        print(f"Error deleting message: {e}")
        raise e
    
@task
def collect_messages(queue_url):
    # attempt to collect all messages from the queue
    logger = get_run_logger()
    messages = []
    while True:
        msg = get_message(queue_url)
        # break if no message retrieved
        if msg is None:
            logger.info("No message retrieved from the queue.")
            break
        if msg and "MessageAttributes" in msg:
            # add message to the list and delete from queue
            order_no = int(msg["MessageAttributes"]["order_no"]["StringValue"])
            word = msg["MessageAttributes"]["word"]["StringValue"]
            messages.append((order_no, word))
            logger.info(f"Added word '{word}' with order_no {order_no} to the list of messages.")
            delete_message(queue_url, msg["ReceiptHandle"])
            logger.info(f"Deleted message with order_no {order_no} from the queue.")
        else:
            logger.info("No more messages in the queue.")
            break
    logger.info(f"Collected {len(messages)} messages from the queue.")
    return messages

@task
def assemble_quote(messages):
    logger = get_run_logger()
    # sort messages by order_no, assembling the quote
    messages.sort(key=lambda x: x[0])
    quote = " ".join([word for order_no, word in messages])
    logger.info(f"Assembled quote: {quote}")
    return quote

@task
def send_solution(phrase):
    # send the solution, uva id, and platform type to the submit url
    logger = get_run_logger()
    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    try:
        response = sqs.send_message(
            QueueUrl=submit_url,
            MessageBody="dp2 prefect message",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': 'vcx4ka'
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': 'prefect'
                }
            }
        )
        logger.info(f"Sent solution to SQS. Phrase: {phrase}")
        logger.info(f"SQS response: {response}")
        return response
    except Exception as e:
        logger.error(f"Error sending solution: {e}")
        raise e

@flow
def quote_assembler_flow():
    # main flow to orchestrate tasks
    logger = get_run_logger()
    sqs_url = populate_queue()
    logger.info(f"SQS URL: {sqs_url}")
    
    monitor_queue(sqs_url)
    messages = collect_messages(sqs_url)
    phrase = assemble_quote(messages)
    send_solution(phrase)
    logger.info(f"Flow complete.")
    return phrase

if __name__ == "__main__":
    quote_assembler_flow()