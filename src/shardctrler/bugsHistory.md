# 在lab4A中遇到较多关于go语法、go特性方面的bug，值得记录
## 值类型深拷贝，引用类型浅拷贝
1. 值类型的数据，默认全部都是深拷贝，Array、Int、String、Struct、Float，Bool。
2. 引用类型的数据，默认全部都是浅拷贝，Slice，`Map`。

## map遍历顺序
Golang中对map的多次遍历得到的序列是不同的。所以如果server在apply来自raft的log时使用了
$$for \; \_ := range \; map$$
会导致不同的机器运行相同的log得到不同的结果。