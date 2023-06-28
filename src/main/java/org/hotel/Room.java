package org.hotel;

public class Room {
    private int id;
    private String name;
    private double price;

    public Room(int pId, String pName, double pPrice){
        id = pId;
        name = pName;
        price = pPrice;
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

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
