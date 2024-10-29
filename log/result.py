import re
import os
from datetime import datetime
from collections import defaultdict

# 每个分片保存自己向其他分片发送的日志条目
def extract_sendtx_logs(log_file, from_shard_start, from_shard_end, to_shard_start, to_shard_end):
    # 确保 from_shard_end >= from_shard_start
    if from_shard_end < from_shard_start:
        raise ValueError("from_shard_end must be greater than or equal to from_shard_start")
    if to_shard_end < to_shard_start:
        raise ValueError("to_shard_end must be greater than or equal to to_shard_start")

    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()

    # 保存提取的哈希值、日志及时间戳
    # hash_values = {
    #     from_shard: {to_shard: [] for to_shard in range(to_shard_start, to_shard_end + 1)}
    #     for from_shard in range(from_shard_start, from_shard_end + 1)
    # }
    extracted_logs = {
        from_shard: {to_shard: [] for to_shard in range(to_shard_start, to_shard_end + 1)}
        for from_shard in range(from_shard_start, from_shard_end + 1)
    }
    timestamp_values_start = defaultdict(lambda: defaultdict(list))

    # 定义日志行的正则表达式模式
    pattern = re.compile(
        rf'time="(?P<timestamp>[^"]+)" level=info msg="generate cross-shard tx, \[Node-(\d+)-\d+\] fromShard (\d+) to (\d+) tx has been generated, tx.hash is (\w+)'
    )

    # 遍历所有日志行，只遍历一次
    for line in lines:
        match = pattern.search(line)
        if match:
            time = match.group(1)
            from_shard = int(match.group(3))  # 提取 fromShard
            to_shard = int(match.group(4))
            tx_hash = match.group(5)          # 提取交易哈希

            # 检查 from_shard 和 to_shard 是否在指定范围内
            if from_shard_start <= from_shard <= from_shard_end and to_shard_start <= to_shard <= to_shard_end:
                extracted_logs[from_shard][to_shard].append(line.strip())  # 保存日志行
                # hash_values[from_shard][to_shard].append(tx_hash)          # 保存哈希值
                timestamp_values_start[from_shard][to_shard].append({
                    'tx_hash': tx_hash,
                    'timestamp': time
                })  # 保存哈希值和时间戳

    # 将提取的日志行写入到相应的输出文件
    for from_shard, to_shards in extracted_logs.items():
        for to_shard, logs in to_shards.items():
            if logs:  # 仅在有日志行的情况下保存文件
                output_file = f"log_{from_shard}_to_{to_shard}.txt"
                with open(output_file, 'w') as file:
                    for log in logs:
                        file.write(log + '\n')

    print(f"GET time start of {from_shard_start} to {from_shard_end} txs.")
    return timestamp_values_start

# 处理 BFT 共识日志信息
def start_bft_consensus_logs(log_file, from_shard_start, from_shard_end,  to_shard_start, to_shard_end, tx_block):
    # 确保 from_shard_end >= from_shard_start
    if from_shard_end < from_shard_start:
        raise ValueError("from_shard_end must be greater than or equal to from_shard_start")
    if to_shard_end < to_shard_start:
        raise ValueError("to_shard_end must be greater than or equal to to_shard_start")


    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()

    timestamp_values_end = defaultdict(lambda: defaultdict(list))

    # 记录每个日志文件已写入的 block_hash
    written_block_hashes = {
        from_shard: {to_shard: set() for to_shard in range(to_shard_start, to_shard_end + 1)}
        for from_shard in range(from_shard_start, from_shard_end + 1)
    }

    # 临时字典来存储 block_hash 和对应的时间戳
    extracted_logs = {}
    time_result = {}

    # 定义正则表达式模式列表，将 block_hash 设置为第三个捕获组
    start_bft_pattern = re.compile(
        rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-(\d+)-\d+\] start bft consensus with new block-(\d+)-\d+-(\w+)\." process=consensus'
    )

    shard_committed_pattern = re.compile(
        rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Shard-(\d+)\] block-(\d+)-\d+-(\w+) committed: \d+/\d+" process=consensus'
    )

    # 遍历所有日志行
    for line in lines:
        start_bft_match = start_bft_pattern.search(line)
        shard_committed_match = shard_committed_pattern.search(line)
        
        if start_bft_match:
            block_hash = start_bft_match.group(4)
            if block_hash not in extracted_logs:
                extracted_logs[block_hash] = []
            extracted_logs[block_hash].append(line.strip()) 
        
        if shard_committed_match:
            # 提取时间戳
            time = shard_committed_match.group(1)
            block_hash = shard_committed_match.group(4)
            if block_hash not in extracted_logs:
                extracted_logs[block_hash] = []
            extracted_logs[block_hash].append(line.strip()) 
            # 存储 block_hash 和时间戳
            time_result[block_hash] = time

    # 使用 tx_block 将 timestamp_values_end 填充为 tx_hash 到时间戳的映射
    for from_shard, to_shards in tx_block.items():
        for to_shard, tx_hashes in to_shards.items():
            for tx_hash in tx_hashes:
                # 获取对应的 block_hash 和时间戳
                block_hash = tx_hash['block_hash']
                if block_hash in time_result and time_result[block_hash]:
                    # 填充 timestamp_values_end，添加字典格式的 tx_hash 和 timestamp
                    timestamp_values_end[from_shard][to_shard].append({
                        'tx_hash': tx_hash['tx_hash'],
                        'timestamp': time_result[block_hash]
                    })
                    # 仅在未写入过相同 block_hash 的情况下写入日志
                if block_hash not in written_block_hashes[from_shard][to_shard]:
                    # 获取对应 block_hash 的日志并写入文件
                    output_file = f"log_{from_shard}_to_{to_shard}.txt"
                    with open(output_file, 'a') as file:
                        for log in extracted_logs[block_hash]:
                            file.write(log + '\n')
                    
                    # 记录已写入的 block_hash
                    written_block_hashes[from_shard][to_shard].add(block_hash)

    print(f"GET time end of {from_shard_start} to {from_shard_end} txs.")
    return timestamp_values_end

def tx_packed_to_block(log_file, from_shard_start, from_shard_end, to_shard_start, to_shard_end):
    # 确保 from_shard_end >= from_shard_start
    if from_shard_end < from_shard_start:
        raise ValueError("from_shard_end must be greater than or equal to from_shard_start")
    if to_shard_end < to_shard_start:
        raise ValueError("to_shard_end must be greater than or equal to to_shard_start")
    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()

    # 保存 交易被打包进的区块
    tx_block = {
        from_shard: {to_shard: [] for to_shard in range(to_shard_start, to_shard_end + 1)}
        for from_shard in range(from_shard_start, from_shard_end + 1)
    }

    extracted_logs = {
        from_shard: {to_shard: [] for to_shard in range(to_shard_start, to_shard_end + 1)}
        for from_shard in range(from_shard_start, from_shard_end + 1)
    }

    # 定义日志行的正则表达式模式
    pattern = re.compile(
        rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-(\d+)-\d+\] Fromshard: (\d+), Transaction Hash: (\w+),Block Hash: (\w+)" process=consensus'
    )

    # 遍历所有日志行，只遍历一次
    for line in lines:
        match = pattern.search(line)
        if match:
            to_shard = int(match.group(2))
            from_shard = int(match.group(3))
            tx_hash = match.group(4)           # 提取 tx.hash
            block_hash = match.group(5)        # 提取 block.hash

            if from_shard_start <= from_shard <= from_shard_end and to_shard_start <= to_shard <= to_shard_end:
                # 保存日志和 block.hash
                extracted_logs[from_shard][to_shard].append(line.strip())  # 保存日志行
                tx_block[from_shard][to_shard].append({'tx_hash': tx_hash, 'block_hash': block_hash})          # 保存哈希值

    # 将提取的日志行写入到相应的输出文件
    for from_shard, to_shards in extracted_logs.items():
        for to_shard, logs in to_shards.items():
            if logs:  # 仅在有日志行的情况下保存文件
                output_file = f"log_{from_shard}_to_{to_shard}.txt"
                with open(output_file, 'a') as file:
                    for log in logs:
                        file.write(log + '\n')

    print("GET the block_hash of tx packed.")
    return tx_block

def calculate_time_differences(time_start, time_end):
    # 用于保存时间差的结果
    time_differences = {}

    # 遍历 time_end 中的每个 from_shard
    for from_shard in time_end:
        if from_shard not in time_differences:
            time_differences[from_shard] = {}

        for to_shard, end_logs in time_end[from_shard].items():
            # 检查是否存在对应的 start_logs
            if from_shard not in time_start or to_shard not in time_start[from_shard]:
                print(f"No start times found for from_shard {from_shard} to to_shard {to_shard}.")
                continue
            
            start_logs = time_start[from_shard][to_shard]
            
            # 将 start_logs 转换为一个哈希值到时间戳的映射
            start_time_map = {log['tx_hash']: log['timestamp'] for log in start_logs}

            # 初始化时间差字典
            if to_shard not in time_differences[from_shard]:
                time_differences[from_shard][to_shard] = []

            # 遍历 end_logs，计算时间差
            for end_log in end_logs:
                tx_hash = end_log['tx_hash']
                end_time = end_log['timestamp']
                
                # 查找对应的开始时间
                start_time = start_time_map.get(tx_hash)
                if start_time:
                    # 计算时间差
                    start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f')
                    end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S.%f')
                    time_diff = (end_dt - start_dt).total_seconds()
                    
                    # 存储时间差
                    time_differences[from_shard][to_shard].append({
                        'tx_hash': tx_hash,
                        'time_difference': round(time_diff, 6)  # 保留6位小数
                    })
    print(time_differences)
    return time_differences

def print_time_differences(time_differences):
    for from_shard, to_shard_logs in time_differences.items():
        for to_shard, logs in to_shard_logs.items():
            print(f"从 {from_shard} 到 {to_shard} 的交易耗时为：")
            total_time = 0
            count = 0
            
            for log in logs:
                tx_hash = log['tx_hash']
                time_diff = log['time_difference']
                if time_diff >= 0:
                    print(f"序号: {count + 1} tx_hash: {tx_hash} 耗时: {time_diff:.6f} 秒")
                    total_time += time_diff
                    count += 1
            
            # 计算平均耗时
            if count > 0:
                average_time = total_time / count
                print(f"从 {from_shard} 到 {to_shard} 的 {count} 笔跨片交易平均交易耗时为: {average_time:.6f} 秒")
            else:
                print(f"从 {from_shard} 到 {to_shard} 没有交易记录")
            print()

def main():
    # 设置shard_id
    from_shard_start = 1001
    from_shard_end = 1016

    to_shard_start = 1001
    to_shard_end = 1016
    

    log_file = "log1.txt"

    # 提取跨片交易生成日志并获取交易哈希值
    time_start = extract_sendtx_logs(log_file, from_shard_start, from_shard_end, to_shard_start, to_shard_end)

    # 获得跨片交易在目的分片被打包进的区块hash
    tx_block = tx_packed_to_block(log_file, from_shard_start, from_shard_end, to_shard_start, to_shard_end)

    # 获得交易对应区块commit的时间
    time_end = start_bft_consensus_logs(log_file, from_shard_start, from_shard_end, to_shard_start, to_shard_end, tx_block)
    time_differences = calculate_time_differences(time_start, time_end)
    print_time_differences(time_differences)


if __name__ == "__main__":
    main()
