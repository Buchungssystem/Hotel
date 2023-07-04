package org.hotel;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.utils.Operations;
import org.utils.Participant;
import org.utils.SendingInformation;
import org.utils.UDPMessage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.UUID;

public class Hotel extends Participant {

    @Override
    public Operations Vote() {
        return Operations.ABORT;
    }

    @Override
    public byte[] book(LocalDate startDate, LocalDate endDate) {

    }


    @Override
    public ArrayList<Object> getAvailableItems(LocalDate startDate, LocalDate endDate) {
        DatabaseConnection dbConn = new DatabaseConnection();
        ArrayList<String> availableRoomIds = new ArrayList<>();
        ResultSet rs = null;
        UDPMessage udpMessage;
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("SELECT * FROM booking");
            rs = stm.executeQuery();

            while(rs.next()){
                LocalDate startDateEntry = LocalDate.parse(rs.getString("startDate"));
                LocalDate endDateEntry = LocalDate.parse(rs.getString("endDate"));
                if (!(startDateEntry.isAfter(endDate) | endDateEntry.isBefore(startDate))) {
                    availableRoomIds.add(rs.getString("roomId"));
                }
            }

            String availableRoomsIds = "";
            for (int i = 0; i < availableRoomIds.size(); i++) {
                if( i < availableRoomIds.size() - 1){
                    availableRoomsIds += availableRoomIds.get(i) + ", ";
                }else{
                    availableRoomsIds += availableRoomIds.get(i);
                }
            }

            stm = con.prepareStatement("SELECT * FROM room WHERE id IS NOT IN (?)");
            stm.setString(1, availableRoomsIds);
            rs = stm.executeQuery();
            ArrayList<Object> availableRooms = new ArrayList<>();
            while(rs.next()){
                availableRooms.add(new Room(rs.getInt("id"), rs.getString("name")));
            }

            return availableRooms;

        }catch(Exception e){
            System.out.println("An Error has occured: " + e.getMessage());
            return null;
        }
    }

    public boolean isAvailable(LocalDate startDate, LocalDate endDate, LocalDate startDateEntry, LocalDate endDateEntry){
        if (!(startDateEntry.isAfter(endDate) | endDateEntry.isBefore(startDate))) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) {

        }
}

