package org.hotel;
import com.fasterxml.jackson.annotation.JsonProperty;


public class Room {
    @JsonProperty("id")
    private int id;

    @JsonProperty("name")
    private String name;


    public Room(int pId, String pName){
        id = pId;
        name = pName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
