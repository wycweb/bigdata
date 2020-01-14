# IOC DI 过程
## IOC的目的
表面目的：面向抽象编程，为了实现可维护的代码，要实现 开闭原则(OCP Open Closed principle)  
### 开闭原则OCP
扩展是开放的、修改是封闭的  
因此尽量 新增业务模块/类 来代替原来的类

开闭原则类似原则：历史替换原则 迪米特法则 IOC DI    
实现开闭原则要面向抽象编程 即interface abstract  
只有有interface abstract才能实现三大特性之一的多态性  
### 为什么事先开闭原则要实现interface 和 abstract
## IOC DI理解
IOC DI不是一下出现的，是通过一定的流程来发展过来的  
第一阶段：最开始interface 面向抽象去编程  
第二阶段：设计模式：使用工厂模式  
第三阶段：出现IOC DI