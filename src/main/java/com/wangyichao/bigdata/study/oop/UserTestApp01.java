package com.wangyichao.bigdata.study.oop;

/**
 * 属性(成员变量) vs 局部变量
 * 1.相同点
 * 1.1 定义的格式是一样的：数据类型 变量名 = 变量值
 * 1.2 先声明、后使用
 * 1.3 变量都有其对应的作用域
 * <p>
 * 2.不同点
 * 2.1 在类中声明的位置不同
 * 属性：直接定义在类的一对{}内
 * 局部变量：声明在方法内、方法形参、代码块内、构造器形参、构造器内部的变量
 * <p>
 * 2.2 权限修饰符的不同
 * 属性：可以在声明时，指明其权限，使用权限修饰符
 * 常用的权限修饰符：private、public、缺省、protected
 * <p>
 * 局部变量：不可以使用权限修饰符
 * <p>
 * 2.3默认初始化值的情况
 * 属性：类的属性、根据其类型，都有默认初始化值
 * 整型（byte、short、int、long）0
 * 浮点型（float、double） 0.0
 * 字符型（char）：0 或 ('\u0000')
 * <p>
 * 引用类型(类、数组、接口)：null
 * <p>
 * 局部变量：没有初始化值
 * 调用的时候一定要显示赋值
 * 特别的：形参在调用时候赋值即可
 * <p>
 * 2.4在内存中加载的位置
 * 属性：加载到堆空间(非static)
 * 局部变量：加载到栈空间
 */
public class UserTestApp01 {


}

class User {

    public String name;
    private int age;
    boolean isMale;

    public void talk(String language) {//language就是形参，局部变量
        System.out.println("使用" + language + "进行交流");
    }

    public void eat() {

        String food = "火锅";//food也是局部变量
        System.out.println("四川人喜欢吃" + food);
    }
}