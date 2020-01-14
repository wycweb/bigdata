package com.wangyichao.bigdata.study.awkward.v2;

import com.wangyichao.bigdata.study.awkward.v2.hero.Camille;
import com.wangyichao.bigdata.study.awkward.v2.hero.Diana;
import com.wangyichao.bigdata.study.awkward.v2.hero.Irelia;

import java.util.Scanner;

public class Main {

    /**
     * 第二版仅仅解决了英雄新增过程的统一
     * 但是仍然未解决改变业务还需要修改主题代码的问题
     */
    public static void main(String[] args) throws Exception {
        String name = Main.getPlayerInput();

        SkillImpl skillImpl;
        switch (name) {
            case "Daiana":
                skillImpl = new Diana();
                break;
            case "Irelia":
                skillImpl = new Irelia();
                break;
            case "Camille":
                skillImpl = new Irelia();
                break;
            default:
                throw new Exception("No hero");
        }

        skillImpl.r();

    }

    private static String getPlayerInput() {
        System.out.println("Enter a Hero's Name");

        Scanner scanner = new Scanner(System.in);
        return scanner.nextLine();
    }

}
