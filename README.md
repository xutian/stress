步骤：
1：进入kafka
2：脚本使用方法 ./sndmsg -c xxx.toml

配置文件说明：

[required]
>eip="10.253.31.238" #对应通过dataproxy发送数据
>
>brokerips=["127.0.0.1:9094","127.0.0.2:9094"] #对应通过kafka发送数据
>
>topic="mpp_bus_pro"
>
>schemaname=2 # 切换schema：1--对应旧的schema;2--对应新的schema
>
>threadsnum=3 # 线程数
>
>recordnum=1 # 每条消息行数
>
>sndnum=20 # 总消息数
>
>runtostop=0 # 总运行时长：单位min
>
[optional]
>flow=false # 是否流量控制,开启后,同时设置令牌间隔时间
>
>flowinterval=0 # 令牌间隔时间:单位ms 
>
[test]
>usemethod=1 # 默认为1,通过dataproxy发送数据;2为通过kafka发送数据 	
---
以下为（按时间运行长稳测试、按发送数量打数量）*（不进行流量控制、20M/s流量控制）排列组合4种模式的样例及说明
---
开启流量控制
---
+ brokerips=["xxx.xxx.xxx.xxx:9094","xxx.xxx.xxx.xxx:9094"]
+ topic="mpp_bus_pro"
+ schemaname=2 
+ threadsnum=1
+ recordnum=3333 # 一个record 3333行，300B，大概1兆
+ sndnum=0
+ runtostop=0.5 # 单位min
+ flow=true 
+ flowinterval=50 # 令牌50ms生成一个
>最后计算出来可以达到19M/s
---
+ brokerips=["xxx.xxx.xxx.xxx:9094","xxx.xxx.xxx.xxx:9094"]
+ topic="mpp_bus_pro"
+ schemaname=2 
+ threadsnum=20
+ recordnum=3333 # 一个record 3333行，300B，大概1兆
+ sndnum=200
+ runtostop=0
+ flow=true 
+ flowinterval=50 # 令牌50ms生成一个
---
非流量控制
---
+ brokerips=["xxx.xxx.xxx.xxx:9094","xxx.xxx.xxx.xxx:9094"]
+ topic="mpp_bus_pro"
+ schemaname=2 
+ threadsnum=20
+ recordnum=3333 # 一个record 3333行，300B，大概1兆
+ sndnum=200
+ runtostop=0
+ flow=false
+ flowinterval=0
---
+ brokerips=["xxx.xxx.xxx.xxx:9094","xxx.xxx.xxx.xxx:9094"]
+ topic="mpp_bus_pro"
+ schemaname=2 
+ threadsnum=200
+ recordnum=3333 # 一个record 3333行，300B，大概1兆
+ sndnum=0
+ runtostop=0.5
+ flow=false
+ flowinterval=0
