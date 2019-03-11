### 分支说明

1. wztyforum分支功能：  从mysql中增量采集论坛，外媒，首页数据进入kafka
2. wisewebTag分支：  从kafka消费数据结合规则库进行分词。分解规则库发送到对应的企业应用库中。结合spark-redis使用