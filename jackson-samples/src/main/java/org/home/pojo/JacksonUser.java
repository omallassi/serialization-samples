package org.home.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by omallassi on 27/05/2016.
 */
public class JacksonUser {

    private String name;
    private int favoriteNumber;
    private String favoriteColor;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("favorite_number")
    public int getFavoriteNumber() {
        return favoriteNumber;
    }

    public void setFavoriteNumber(int favoriteNumber) {
        this.favoriteNumber = favoriteNumber;
    }

    @JsonProperty("favorite_color")
    public String getFavoriteColor() {
        return favoriteColor;
    }

    public void setFavoriteColor(String favoriteColor) {
        this.favoriteColor = favoriteColor;
    }
}
