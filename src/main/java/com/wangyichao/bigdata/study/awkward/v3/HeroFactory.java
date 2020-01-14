package com.wangyichao.bigdata.study.awkward.v3;

import com.wangyichao.bigdata.study.awkward.v3.hero.Diana;
import com.wangyichao.bigdata.study.awkward.v3.hero.Irelia;

public class HeroFactory {

    /**
     * 工厂方法生产对象
     * 下面代码仍然存在各种实例化
     * 可以通过抽象工厂来完成多次实例化的情况，使用抽象工厂是对SkillImpl的再次抽象
     *
     * 关于变化的思考：
     * 为什么这一块的代码会不断变化，不断实例化
     * 根本的本质是输入的内容在变化，要根据变化的内容不断地实例化新的内容
     * 如果输入直接为实例化的内容，那么该段代码也能做到稳定不变化
     */
    public static SkillImpl getHero(String name) throws Exception {
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

        return skillImpl;
    }
}
