import re
import os
from datetime import datetime

def extract_sendtx_logs(log_file, from_shard, to_shard_start, to_shard_end):
# 确保 to_shard_end >= to_shard_start
    if to_shard_end < to_shard_start:
        raise ValueError("to_shard_end must be greater than or equal to to_shard_start")

    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()


    extracted_logs = []
    # 保存提取的哈希值
    hash_values = {}
    # 保存提取的哈希值及其对应的时间戳
    timestamp_values = {}

    # 遍历 to_shard 的范围
    for to_shard in range(to_shard_start, to_shard_end + 1):
        # 创建输出文件名
        output_file = f"log_{from_shard}_to_{to_shard}.txt"

        # 定义日志行的正则表达式模式
        # 匹配日志中的时间戳和 tx.hash
        pattern = re.compile(
            rf'time="(\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}\.\d+)" .*fromShard {from_shard} to {to_shard} tx has been generated, tx.hash is (\w+)'
        )
        
        # 初始化空列表，防止 KeyError
        hash_values[to_shard] = []
        timestamp_values[to_shard] = []

        # 过滤出符合模式的日志行，并同时提取哈希值和时间戳
        for line in lines:
            match = pattern.search(line)
            if match:
                extracted_logs.append(line.strip())  # 过滤日志行
                hash_values[to_shard].append(match.group(2))  # 提取哈希值
                timestamp_values[to_shard].append({
                    'tx_hash': match.group(2),
                    'timestamp': match.group(1)
                })  # 提取哈希值和时间戳

        # 将提取的日志行写入到指定的输出文件
        with open(output_file, 'w') as file:
            for log in extracted_logs:
                file.write(log + '\n')

    print(f"1 Logs extracted and saved to files for to_shard range {to_shard_start} to {to_shard_end}.")
    
    # 返回提取的哈希值
    return hash_values, timestamp_values

def start_bft_consensus_logs2(log_file, from_shard, to_shard, block_hash_list):
    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()

    # 定义输出文件名
    output_file = f"log_{from_shard}_to_{to_shard}.txt"

    # 初始化返回结果的列表
    consensus_timestamps = []

    # 遍历 block_hash_list 以处理每个 block_hash
    for tx_hash, block_hash in block_hash_list:
        # 定义 BFT 共识日志信息的正则表达式模式
        start_bft_pattern = re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-{to_shard}-\d+\] start bft consensus with new block-{to_shard}-\d+-{block_hash}\." process=consensus'
        )
        
        shard_committed_pattern = re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Shard-{to_shard}\] block-{to_shard}-\d+-{block_hash} committed: \d+/\d+" process=consensus'
        )

        # 保存提取的原始日志行
        extracted_logs = []
        committed_timestamp = None

        # 过滤出符合任一模式的日志行
        for line in lines:
            start_bft_match = start_bft_pattern.search(line)
            shard_committed_match = shard_committed_pattern.search(line)
            
            if start_bft_match:
                extracted_logs.append(line.strip())
            
            if shard_committed_match:
                extracted_logs.append(line.strip())
                # 提取时间戳
                committed_timestamp = shard_committed_match.group('timestamp')
                break  # 一旦找到 committed 日志，可以停止搜索

        # 将提取的信息写入到指定的输出文件
        with open(output_file, 'a') as file:
            for info in extracted_logs:
                file.write(info + '\n')

        # 将 tx_hash 和对应的时间戳保存到结果列表中
        consensus_timestamps.append({
            'tx_hash': tx_hash,
            'timestamp': committed_timestamp  # 如果未找到 committed 日志，值可能为 None
        })

    # 返回包含 tx_hash 和时间戳的数组
    return consensus_timestamps

def start_bft_consensus_logs2(log_file, from_shard, to_shard, block_hash_list):
    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()

    # 定义输出文件名
    output_file = f"log_{from_shard}_to_{to_shard}.txt"

    # 保存提取的日志和最终的时间戳
    extracted_logs = []
    tx_hash_timestamps = []

    # 提前检查包含相关的关键字，提高效率
    relevant_patterns = ['start bft consensus', f'[Shard-{to_shard}]', f'block-{to_shard}-']

    # 遍历日志行
    for line in lines:
        # 提前过滤不包含关键字的日志行
        if 'start bft consensus' not in line:
             continue

        # 遍历 block_hash_list，生成对应的正则模式
        for item in block_hash_list:
            tx_hash = item['tx_hash']
            block_hash = item['block_hash']

            # 定义 BFT 共识日志信息的正则表达式模式
            start_bft_pattern = re.compile(
                rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-{to_shard}-\d+\] start bft consensus with new block-{to_shard}-\d+-{block_hash}\." process=consensus'
            )

            shard_committed_pattern = re.compile(
                rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Shard-{to_shard}\] block-{to_shard}-\d+-{block_hash} committed: \d+/\d+" process=consensus'
            )

            # 优化正则匹配
            start_bft_match = start_bft_pattern.search(line)
            shard_committed_match = shard_committed_pattern.search(line)

            if start_bft_match:
                # 提取并保存日志行
                extracted_logs.append(line.strip())

            if shard_committed_match:
                # 提取并保存日志行
                extracted_logs.append(line.strip())
                
                # 提取并保存 committed 的时间戳
                committed_timestamp = shard_committed_match.group('timestamp')
                tx_hash_timestamps.append({'tx_hash': tx_hash, 'timestamp': committed_timestamp})
                break  # 找到 committed 日志后可以停止处理该 block_hash

    # 将提取的信息写入到指定的输出文件
    with open(output_file, 'a') as file:
        for info in extracted_logs:
            file.write(info + '\n')

    # 返回 tx_hash 和其对应的时间戳
    return tx_hash_timestamps

def tx_packed_to_block(log_file, from_shard, to_shard_start, to_shard_end, tx_hashes):
    # 确保 to_shard_end >= to_shard_start
    if to_shard_end < to_shard_start:
        raise ValueError("to_shard_end must be greater than or equal to to_shard_start")

    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()

    # 保存共识阶段的时间戳
    consensus_timestamps = {}

    for to_shard in range(to_shard_start, to_shard_end + 1):
        # 定义输出文件名
        output_file = f"log_{from_shard}_to_{to_shard}.txt"
        
        # 从 tx_hashes 中获取目标 to_shard 的交易哈希
        tx_hash_list = tx_hashes.get(to_shard, [])
        
        tx_patterns = {tx_hash: re.compile(rf'time="[^"]+" level=info msg="\[Node-{to_shard}-\d+\] Transaction Hash: {tx_hash},Block Hash: (?P<block_hash>[0-9a-fA-F]+)" process=consensus')
                              for tx_hash in tx_hash_list}

        extracted_logs = []
        block_hash_list = []
        # 遍历日志行
        for line in lines:
            # 不存在该字符串，直接转到下一行
            if ',Block Hash:' not in line:
                continue
            for tx_hash, tx_pattern in tx_patterns.items():
                match = tx_pattern.search(line)
                if match:
                    # 提取并保存日志行
                    extracted_logs.append(line.strip())
                    # 提取区块哈希值
                    block_hash = match.group('block_hash')
                    # 将 tx_hash 和 block_hash 组成字典存入列表
                    block_hash_list.append({'tx_hash': tx_hash, 'block_hash': block_hash})
                    break

        # 将提取的日志行追加到指定的输出文件
        with open(output_file, 'a') as file:
            for log in extracted_logs:
                file.write(log + '\n')

        # 调用 start_bft_consensus_logs2 函数并获取时间戳
        consensus_timestamp_list = start_bft_consensus_logs2(log_file, from_shard, to_shard, block_hash_list)
        
        consensus_timestamps[to_shard] = []
        # 将每个 tx_hash 和其对应的 timestamp 追加到 consensus_timestamps 中
        for consensus_entry in consensus_timestamp_list:
            consensus_timestamps[to_shard].append({
                'tx_hash': consensus_entry['tx_hash'],
                'timestamp': consensus_entry['timestamp']
            })

    print("2 to_shard Logs extracted and saved.")
    # 返回共识阶段的时间戳数组
    return consensus_timestamps

def calculate_time_differences(time_start, time_end):
    # 用于保存时间差的结果
    time_differences = {}

    # 遍历 time_end 中的每个 shard_id
    for shard_id, end_logs in time_end.items():
        if shard_id not in time_start:
            print(f"No start times found for shard {shard_id}.")
            continue
        
        start_logs = time_start[shard_id]
        
        # 将 start_logs 转换为一个哈希值到时间戳的映射
        start_time_map = {log['tx_hash']: log['timestamp'] for log in start_logs}

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
                if shard_id not in time_differences:
                    time_differences[shard_id] = []
                time_differences[shard_id].append({
                    'tx_hash': tx_hash,
                    'time_difference': round(time_diff, 6)  # 保留6位小数
                })

    return time_differences

def print_time_differences(time_differences):
    for shard_id, logs in time_differences.items():
        print(f"{shard_id} 交易耗时为：")
        total_time = 0
        count = 0
        for log in logs:
            tx_hash = log['tx_hash']
            time_diff = log['time_difference']
            if time_diff >= 0:
                print(f"序号: {count+1} tx_hash: {tx_hash} 耗时: {time_diff:.6f} 秒")
                total_time += time_diff
                count += 1
        
        # 计算平均耗时
        if count > 0:
            average_time = total_time / count
            print(f"目的分片为{shard_id}的{count}笔跨片交易平均交易耗时为: {average_time:.6f} 秒")
        else:
            print(f"{shard_id} 没有交易记录")
        print()

def main():
    # 设置shard_id
    from_shard = 1002
    to_shard_start1 = 1001
    to_shard_end1 = 1001

    # to_shard_start2 = 1021
    # to_shard_end2 = 1040
    
    # 提取跨片交易生成日志并获取交易哈希值
    tx_hashes, time_start = extract_sendtx_logs("log.txt", from_shard, to_shard_start1, to_shard_end1)

    # 目的分片
    time_end = tx_packed_to_block("log.txt", from_shard, to_shard_start1, to_shard_end1, tx_hashes)
    time_differences = calculate_time_differences(time_start, time_end)
    print_time_differences(time_differences)


if __name__ == "__main__":
    main()
