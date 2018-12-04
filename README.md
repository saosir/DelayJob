# DelayJob

基于python异步框架tornado和redis实现的延迟任务队列

参考

- 有赞延迟队列设计 https://tech.youzan.com/queuing_delay/
- delay-queue golang版本实现 https://github.com/ouqiang/delay-queue

## 运行

目前支持python2.7

    git clone https://github.com/saosir/DelayJob.git
    cd DelayJob
    python main.py --config config.py

## 使用场景

### 配置下发

Web界面修改配置后，需要下发配置文件给后台集群，如果短时间内在界面频繁修改，可能会造成许多无用的下发操作，因为生效的是最后一次修改的配置文件。通过添加延迟任务可以有效避免这个问题，当界面修改配置之后，后台更新库表后只需要创建一个延迟job用于下发配置，在job存在情况下如果有配置更新，后台只需要更新库表即可，当job变为可执行状态由worker执行配置下发。

### 资源回收

用户申请某计算资源1个月，使用期限到期系统应自动回收。在申请资源成功之后，后台创建一个延迟1个月的job，由worker执行资源回收并通知后台

## 接口说明

### 添加任务

    POST /push

```json
{
  "id": "15702398321",
  "topic": "topic-test",
  "delay": 3600,
  "ttr": 120,
  "body": ""
}
```

  参数名 |     类型    |     说明     |        备注       
---------|----------|---------|---------
 id | string | job全局唯一标识 | 
 topic | string | job所属topic |
 delay | int | job延迟执行时间 | 单位秒
 ttr | int | job执行时间，在指定时间内未执行完成，后台重新调度此job| 单位秒
 body | string | job内容，根据业务需要，由worker解析处理 | 


### 获取可执行任务

    POST /pop
    
```json
{
  "topic": "task",
  "timeout": 0
}
```

支持http长连接等待可执行job

- topic 获取topic下可执行job
- timeout DelayJob支持http长连接，在指定时间下没有可执行job，后台自动断开worker连接并返回空结果，最大设置为30秒，为0或者不传递timeout字段，则立即返回结果

### 完成任务

    POST /finish
    
 ```json
{
  "id": "job-id" 
}
 ```
 
### 查询任务

    POST /get

```json
{
  "id": "job-id"
}
```