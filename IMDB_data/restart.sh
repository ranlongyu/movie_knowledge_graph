#!/bin/bash

pidof find_people_info.py # 检测程序是否运行
while [ $? -ne 0 ]    # 判断程序上次运行是否正常结束
do
    echo "Process exits with errors! Restarting!"
    source activate reptilian
    python find_people_info.py    #重启程序
done
echo "Process ends!"
