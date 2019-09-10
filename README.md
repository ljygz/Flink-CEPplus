#  Flink1.8.1-CEPplus

基于Flink1.8.1源码 为CEP模块添加 逻辑动态注入功能

    功能描述: 使用CEP作为复杂事件处理引擎时,当逻辑频繁发生修改,以及阈值频繁调整时
              整个程序需要停止后,修改代码,重新打包程序然后给集群提交，无法实现逻辑
              动态修改和外部动态注入,目前已经实现了CEP逻辑动态注入，基于消息驱动逻
              辑修改，可以手动往source端注入特定消息实现细腻度控制逻辑注入感知     

为Client端API中PatternStream添加方法registerListen(CepListen<T> cepListen)  注意必须在select方法之前调用

cepListen对象需要实现接口CepListen

接口方法

        Boolean needChange()      每条数据会调用这个方法，用于确定这条数据是否会触发规则更新
        Pattern returnPattern()   触发更新时调用，用于返回新的pattern作为新规则

整个工程编译以后可以直接运行
    
    mvn clean install -Dmaven.test.skip=true

功能测试代码

    org.apache.flink.streaming.examples.cep.Driver

