finalBigData文件夹中包含实验所有代码：按照任务一至六分别在lab1至lab6中。程序入口为Main类。
程序通过Maven进行管理，通过输入“maven clean package”命令并等待编译完成即可。生成的jar包为target目录的finalBigData-1.0-SNAPSHOT.jar。
初始状态jar包可以直接在该目录下找到。将其上传至集群，直接运行hadoop jar finalBigData-1.0-SNAPSHOT.jar。
注意由于分词时的要求，在程序所在目录下要求有default.dic文件，在我们集群上已经上传，如需重新上传在本目录下可以找到。
运行后会在hdfs中生成out1、out2、out3、out4、out5_iter1至9、out6_1、out6_2等文件夹，其中各文件夹下part-r-00000为输出结果。
由于程序测试运行为周宇航同学负责，如果任何疑问请联系1223870886@qq.com。感谢您的阅读。
