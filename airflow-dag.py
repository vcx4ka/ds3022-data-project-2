# airflow DAG goes here
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests, boto3, time, logging

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/vcx4ka"
sqs = boto3.client('sqs', region_name='us-east-1')
logger = logging.getLogger(__name__)

def populate_queue(**context):
    #POST to API to scatter 21 SQS messages. Return queue URL.

    payload = requests.post(url).json()
    queue_url = payload["sqs_url"]
    logger.info(f"Queue populated. URL: {queue_url}")

    context["ti"].xcom_push(key="queue_url", value=queue_url)
    return queue_url

def monitor_queue(**context):
    #Poll queue attributes until all 21 messages become available.
    queue_url = context["ti"].xcom_pull(task_ids='populate_queue', key='return_value')

    waited = 0

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
        if (visible + delayed + not_visible >= 21) and (delayed == 0):
            logger.info(f"All messages are now available in the queue. Waited {waited} total seconds for messages to arrive.")
            return

        time.sleep(15)
        waited += 15

def get_message(queue_url):
    #Return ONE visible message, or None if queue is empty.
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=2
    )

    if "Messages" not in response:
        logger.info("No messages in the queue.")
        return None
    
    # log message details if a message is received
    logger.info(f"Message received successfully! Message details below...")
    logger.info(f"MessageAttributes: {response['Messages'][0]['MessageAttributes']}")
    logger.info(f"Order No: {response['Messages'][0]['MessageAttributes']['order_no']['StringValue']}")
    return response["Messages"][0]

def delete_message(queue_url, receipt_handle):
    # delete message from queue using receipt handle.
    try:
        response = sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        logger.info(f"Deleting message from queue. Response: \n{response}")
    except Exception as e:
        print(f"Error deleting message: {e}")
        raise e
    
def collect_messages(**context):
    # get all messages, parse attributes, delete them, return list
    queue_url = context["ti"].xcom_pull(task_ids="populate_queue")
    messages = []

    while True:
        msg = get_message(queue_url)
        # break if no message retrieved
        if msg is None:
            logger.info("No message retrieved from the queue.")
            break
        if msg and "MessageAttributes" in msg:
            # add message to list and delete from queue
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
    context["ti"].xcom_push(key="messages", value=messages)

def assemble_quote(**context):
    # sort messages by order_no, assembling the quote
    messages = context["ti"].xcom_pull(key="messages")
    phrase = " ".join([word for order_no, word in messages])
    logger.info(f"Assembled quote: {phrase}")

    context["ti"].xcom_push(key="phrase", value=phrase)

def send_solution(**context):
    # send the solution, uva id, and platform type to the submit url
    phrase = context["ti"].xcom_pull(key="phrase")
    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    
    try:
        response = sqs.send_message(
            QueueUrl=submit_url,
            MessageBody="dp2 airflow message",
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
                        'StringValue': 'airflow'
                    }
                }
        )
        logger.info(f"Sent solution. Phrase: {phrase}")
        logger.info(f"Response: {response}")
        return response
    except Exception as e:
        logger.error(f"Error sending solution: {e}")
        raise e
    
    
# ----------------------- DAG DEFINITION -----------------------

default_args = {
    "owner": "vcx4ka",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dp2_quote_assembler",
    default_args=default_args,
    description="DP2 Quote Assembler in Airflow",
    schedule=None,   # run manually in Airflow
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="populate_queue",
        python_callable=populate_queue,
    )

    t2 = PythonOperator(
        task_id="monitor_queue",
        python_callable=monitor_queue,
    )

    t3 = PythonOperator(
        task_id="collect_messages",
        python_callable=collect_messages,
    )

    t4 = PythonOperator(
        task_id="assemble_quote",
        python_callable=assemble_quote,
    )

    t5 = PythonOperator(
        task_id="send_solution",
        python_callable=send_solution,
    )
    # Define task dependencies
    t1 >> t2 >> t3 >> t4 >> t5