# Yarn app 应用程序实现demo
简单的Yarn applicaiton demo, 程序执行时会根据用户指定的节点个数（节点随机选择），执行一条命令。
目前这条命令被写死在worker的启动脚本里。
此工程实现了YarnApplication 简单逻辑，更复杂的失败重试，指定节点，子作业调度均没有实现。

由于没有经过严格测试，程序中会有BUG。：） 

#编译命令
mvn package -DskipTests


#程序执行方法
1. 将编译好的项目文件夹直接copy 到集群或集群上的一个客户端上
2. 进入bin 目录执行如下命令提交作业
bin/submit.sh 2 hdfs://clustername/tmp

#程序执行结果查看
yarn logs -application appid

因为没有实现Web界面，所以只能通过日志查看



blog.iwantfind.com
www.iwantfind.com


http://blog.iwantfind.com/archives/109
