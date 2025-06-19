import os
import re 

TRANSACTIONS_PATH = os.path.join("/home", "dyscarnate", "work", "samsung", "data", "transactions")

def get_latest_block_number():
    filenames = os.listdir(TRANSACTIONS_PATH)
    max_block = 0
    for filename in filenames:
        numbers = re.findall(r'\d+', filename)
        if numbers:
            end_block = int(numbers[-1])
            max_block = max(max_block, end_block)
    return max_block

print(get_latest_block_number())