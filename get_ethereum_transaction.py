import os
import re

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup

TRANSACTIONS_PATH = os.path.join("/home", "dyscarnate", "work", "samsung", "data", "transactions")
BLOCK_RANGE = 2000
RPC_API_LIST = [#'https://go.getblock.io/8248f7244bc941ff90399364d6fae3ea',
                'https://mainnet.infura.io/v3/92151f380bd8405ca25fd92b457474e7',
                #'https://eth-mainnet.blastapi.io/a2e303b4-4f2a-44f7-9b26-b712be9d8ede',
                'https://eth-mainnet.nodereal.io/v1/c262488a5d97431cb8fdb8224ea9bb66',
                'https://lb.drpc.org/ogrpc?network=ethereum&dkey=AsU4RhkUuUaTjlrqzM74yZJ6eCDfE4AR8JlaKjrWkQAY',
                'https://eth.api.onfinality.io/rpc?apikey=712272ea-cedc-44e3-b924-92b218bcb696',
                'https://api.noderpc.xyz/rpc-mainnet/4jnyMOwYTdWQzullyR4TPB4XebUjdnC9Prvu2quERhY',
                'https://node.exaion.com/api/v1/c27ce59b-0ceb-4002-9090-018d5c38e989/rpc',
                'https://rpc.ankr.com/eth/c9f429c1b661a88ef037370d06e5255e2b3f8221880f7afbb360df4996592cc5',
                'https://ethereum-rpc.publicnode.com/660a7cabbddc4250ccfbfe974e3058f5ca7faf8bc851bf3a904b84a19ea0d335',
                ]
                

PUBLIC_ENDPOINT_LIST = ['']


class TransactionRequest:
    def __init__(self, rpc, start_block, end_block, requests_per_second=5, chunk_size=500, output_dir=TRANSACTIONS_PATH):
        self.rpc = rpc
        self.start_block = start_block
        self.end_block = end_block
        self.requests_per_second = requests_per_second
        self.chunk_size = chunk_size
        self.output_dir = output_dir
    
    def build(self):
        return (
            f"cryo transactions "
                f" --rpc {self.rpc}"
                f" --blocks {self.start_block}:{self.end_block}"
                f" --requests-per-second {self.requests_per_second}"
                f" --chunk-size {self.chunk_size}"
                f" --output-dir {self.output_dir}"
        )
    


def get_latest_block_number():
    filenames = os.listdir(TRANSACTIONS_PATH)
    max_block = 0
    for filename in filenames:
        numbers = re.findall(r'\d+', filename)
        if numbers:
            end_block = int(numbers[-1])
            max_block = max(max_block, end_block)
    return max_block


def get_blocks_to_extract(init_block):
        
    start_block = init_block + 1
    end_block = start_block + BLOCK_RANGE - 1

    return start_block, end_block



def build_cryo_commands(**kwargs):
    commands = []
    last_block = get_latest_block_number()
    for rpc in RPC_API_LIST:
        start_block, end_block = get_blocks_to_extract(last_block)
        last_block += BLOCK_RANGE
        
        command = TransactionRequest(rpc, start_block, end_block).build()
        commands.append(command)
    return commands 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'start_date': datetime(2023, 1, 1),
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'get_eth_transactions',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    concurrency=len(RPC_API_LIST),
    max_active_tasks=len(RPC_API_LIST)
) as dag:
    generate_task = PythonOperator(
        task_id = 'build_cryo_commands',
        python_callable= build_cryo_commands,
        provide_context = True
    )

    with TaskGroup("execute_commands") as execute_group:
        BashOperator.partial(
            task_id='execute_command',
            execution_timeout=timedelta(minutes=30)
        ).expand(
            bash_command=build_cryo_commands()
        )







