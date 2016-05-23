package org.home.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by oliv on 23/05/2016.
 */
public class MyPojo {
    public String name;
    public int age;

    @JsonProperty("name")
    public String getMyName() {
        return name;
    }

    public void setMyName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "MyPojo{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
