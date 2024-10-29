#!/bin/bash

source_dir1="log.txt"
target_dir="/home/zzl/log/log_1s/"
password="jJ?6V*M=dqRX"  # 请替换为您的密码

servers=(
  "10.156.169.19"
)

for server in "${servers[@]}"
do
  echo "Transferring files to server: $server"
  sshpass -p "$password" scp $source_dir1 zzl@$server:$target_dir
  echo "Transfer complete for server: $server"
done