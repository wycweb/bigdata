package com.wangyichao.bigdata.study;

public class BaseApp01 {

    //java基础语法:控制语句、集合操作
    public static void main(String[] args) {
        test01(7);
        test02(1);
        test02(3);
        test03();
    }

    /**
     * 流程控制，有三种基本流程结构
     * 1.顺序结构：从上到下 顺序执行 没有任何判断、跳转
     * 2.分支结构：
     * if else
     * switch case
     * 3.循环结构
     * for
     * while
     * do-while
     */

    // if else
    private static void test01(int number) {

        if (number <= 10 && number > 6) {
            System.out.println("符合");
        } else {
            System.out.println("不符合");
        }
    }

    // switch case
    // 注意要使用break，否则会一直执行
    private static void test02(int number) {

        switch (number) {
            case 0:
                System.out.println(0);
                break;
            case 1:
                System.out.println(1);
                break;
            case 2:
                System.out.println(2);
                break;
            default:
                System.out.println("other number");

        }
    }

    //for 循环
    private static void test03() {

        for (int i = 1; i <= 3; i++) {
            System.out.println("hello i");
        }

        int j = 1;
        do {

            System.out.println("hello j");
            j++;
        } while (j <= 3);

        int k = 1;
        while (k <= 3) {

            System.out.println("hello k");
            k++;
        }
    }

    /**
     * & 和 && 的异同
     * 相同：一假则假
     * 不同：&&具有短路的功能 &还可以用作位运算符（待学习）
     *
     * | 和 || 的异同
     * 相同：一真则真
     * 不同：||具有短路功能
     *
     * 位运算符： << >> <<< >>> （了解，很少用）
     */
}
