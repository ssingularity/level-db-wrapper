# Benchmark

Benchmark模拟了多个ATM处理多个事务，其中一些ATM间的事务会产生竞争，有些ATM之间会产生死锁。我们会并行运行两个ATM，计算每个ATM完成所有事务所需的时间和全部ATM都完成所需要的时间。

| Transaction | ATM 1                        | ATM 2                        | ATM 3                        |
| ----------- | ---------------------------- | ---------------------------- | ---------------------------- |
| 1           | READ 1, PUT 1                | READ 6, PUT 6                | READ 6, PUT 6                |
| 2           | READ 2, READ 3, PUT 2, PUT 3 | READ 4, READ 2, PUT 2, PUT 4 | READ 3, READ 2, PUT 2, PUT 3 |
| 3           | READ 5, PUT 5, READ 5        | READ 7, PUT 7, READ 7        | READ 7, PUT 7, READ 7        |
| 4           | PUT 10, PUT 11, PUT 12       | PUT 14, PUT 13, PUT 12       | PUT 13, PUT 12, PUT 12       |

_ATM 2与ATM 1有竞争关系，但不会产生死锁；ATM 3与ATM 1会产生死锁。_