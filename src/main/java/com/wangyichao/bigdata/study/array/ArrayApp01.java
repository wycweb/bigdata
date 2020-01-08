package com.wangyichao.bigdata.study.array;

public class ArrayApp01 {

    /**
     * 数组概念：相同类型的数据按照一定顺序排列的结合
     * 数组名、元素、下表(索引)、数组长度
     * 数组会在内存中开辟出一个连续的空间
     * 数组的长度一旦确定，就不能修改
     * 数组的分类：
     * 1.按照维度：一维数组 二维数组
     * 2.按照元素的类型：基本数据类型元素数组、引用类型元素数组
     * <p>
     * 数组默认初始化值：
     * 整形：   int 0
     * 浮点：   float 0.0
     * char:   0 或 '\u0000'，不是 '0'
     * 布尔：   false
     * string：  null 注意不是 "null"
     * <p>
     * 数组的内存结构会在jvm中
     *
     * 数组的弊端：
     * 1.数组初始化后，长度就不可不了，不便于扩展
     * 2.数组中的属性和方法少，不便于进行增删
     * 3.数组存储类型单一
     */
    public static void main(String[] args) {

        //初始化只有以下两种方式，要么静态初始化、要么动态初始化，初始化之后长度是不能改变的
        int[] ids;//声明：声明一个int型的数组
        ids = new int[]{1, 2, 3, 4, 6};//静态初始化

        String[] names = new String[3];//动态初始化
        names[0] = "张三";
        names[1] = "李四";
        names[2] = "王二";
        //names[3] = "王二"; 注意：这么写会报错，会报越界的错误

        //数组循环遍历
        for (int j = 0; j < names.length; j++) {
            System.out.println(names[j]);
        }
        for (String name : names) {
            System.out.println(name);
        }

        //二维数组
        int[][] arr1 = new int[][]{{1, 2, 3}, {4, 5, 6}};
        String[][] arr2 = new String[3][2];
        arr2[0][0] = "zhangsan";

        System.out.println(arr1[0][1]);

        //二维数组遍历
        for (int i = 0; i < arr1.length; i++) {
            for (int j = 0; j < args[i].length(); j++) {
                System.out.println(arr1[i][j]);
            }
        }

        System.out.println(arr1[0]);//输出的是数组的地址值

    }
}
