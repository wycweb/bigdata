package com.wangyichao.bigdata.study.oop;

/**
 * 一 设计类就是设计类的成员
 * 属性 = 成员变量 = field = 域、字段
 * 方法 = 成员方法 = 函数
 * <p>
 * 创建类的对象 = 类的实例化 = 实例化类
 * <p>
 * 二 类和对象的使用
 * 1.创建类，设计类的成员
 * 2.创建类的对象
 * 3.通过"对象.属性" 或 "对象.方法" 调用对象的结构
 * <p>
 * 三 如果创建了一个类的多个对象，则每个对象都独立的拥有一套类的属性(非static)
 * 意味着：如果我们修改一个对象的属性a,则不影响另外一个类的属性a
 *
 */
public class PersonTestApp01 {

    public static void main(String[] args) {

        //创建Person类的对象
        Person p1 = new Person();

        //调用对象的结构:属性、方法
        //调用属性："对象 属性"
        p1.name = "张三";
        p1.isMale = true;

        //调用属性
        System.out.println(p1.name);

        //调用方法
        p1.eat();
        p1.sleep();
        p1.talk("Chinese");

        //************************************
        Person p2 = new Person();
        System.out.println(p2.name); //null
        System.out.println(p2.isMale);//false

        //************************************
        Person p3 = p1;

        p3.age = 10;
        p3.name = "李四";

        System.out.println(p3.age);

        //将p1变量保存的对象地址赋值给p3，导致p1和p3指向了堆空间中的同一个对象，因此设置p3.name="李四"，则p1.name 也会变为 ""李四
        System.out.println(p3.name);
        System.out.println(p1.name);



    }
}

class Person {

    //属性
    String name;
    int age;
    boolean isMale;

    //方法
    public void eat() {
        System.out.println("人可以吃饭");
    }

    public void sleep() {
        System.out.println("人可以睡觉");
    }

    public void talk(String language) {
        System.out.println("人可以说话，使用的语言是:" + language);
    }
}