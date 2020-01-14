package com.wangyichao.bigdata.study.awkward.v1;

import com.wangyichao.bigdata.study.awkward.v1.hero.Camille;
import com.wangyichao.bigdata.study.awkward.v1.hero.Diana;
import com.wangyichao.bigdata.study.awkward.v1.hero.Irelia;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        String name = Main.getPlayerInput();

        /**
         * 这种方式新增英雄就要改整体代码，尽量保证主体方法不改变
         * 这种方式会每次都要改动主题代码，而且各个英雄类中有大量程序代码
         */
        switch (name) {
            case "Diana":
                Diana diana = new Diana();
                diana.r();
                break;
            case "Irelia":
                Irelia irelia = new Irelia();
                irelia.r();
                break;
            case "Camille":
                Camille camille = new Camille();
                camille.r();
                break;
        }

    }

    private static String getPlayerInput() {
        System.out.println("Enter a Hero's Name");

        Scanner scanner = new Scanner(System.in);
        return scanner.nextLine();
    }

}
