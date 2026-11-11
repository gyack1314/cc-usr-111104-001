# 定位原始流约定

帧遵循RTCM3的传输封装，CRC多项式为0x1864CFB，初值为零，不反射，无末尾异或，网络字节序。CRC覆盖三字节头和载荷。消息内容是合成字节，无定位意义。`session0.bin`含噪声、重复帧、损坏帧和未结束尾部；`session1.bin`含零长和一字节载荷。

TCP来源启动命令为 `python -m source_lab.tcp sources/streams.json --port 8766`，每个连接顺次消耗一个session，全部消耗后连接即关闭。调整write_sizes只改变写入分块，不改变字节。发生器只有发送职责。`source_lab.rtcm`只有CRC和封装原语，`workbench`只有原始字节存储，不处理帧索引或连接档案。
