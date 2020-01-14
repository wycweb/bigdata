package com.wangyichao.bigdata.study.awkward.v3;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws Exception {
        String name = Main.getPlayerInput();

        /**
         * 剔除掉main函数中的实例化过程
         * 未来新增英雄的时候，main方法无需改动
         * main方法就完成了main方法完成了OCP
         * 但是HeroFactory仍是需要改动
         *
         * 但严格上来说 getHero也仅仅是使用了静态类，和new class仍是很类似的方式
         */
        SkillImpl skillImpl = HeroFactory.getHero(name);
        skillImpl.r();
    }

    private static String getPlayerInput() {
        System.out.println("Enter a Hero's Name");

        Scanner scanner = new Scanner(System.in);
        return scanner.nextLine();
    }

}
