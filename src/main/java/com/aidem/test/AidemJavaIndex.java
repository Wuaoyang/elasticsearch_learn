package com.aidem.test;

import lombok.*;

import java.util.Date;

/**
 * 实体类
 *
 * @author aosun_wu
 * @date 2020-11-22 21:41
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
class AidemJavaIndex {

    private Integer id;
    private Integer age;
    private Date birthday;
    private String name;

}
