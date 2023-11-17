---
GetShard 发出shard后，不立即删除shard。但是必须记住拿到shard的groupID，只有该group才能继续拿shard。
避免了如下问题：时期1，shard由A管理，时期2，shard由B管理，时期3由C管理。从时期1到时期2，B从A Getshard，B没来得及回复A让A删除，就到了时期3。而恰好C没有感受到到时期2，直接从时期1到时期3，C会向A Getshard，A应该拒绝。
---
不能跳跃configNum。√
可以跳跃configNum，X


