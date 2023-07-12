package org.hotel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.cj.x.protobuf.MysqlxPrepare;
import org.utils.*;

import javax.xml.crypto.Data;
import java.net.DatagramSocket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.UUID;

public class Hotel extends Participant {

    public DatagramSocket dgSocket;

    public Hotel() {
        try {
            dgSocket = new DatagramSocket(Participant.hotelPort);
        } catch(Exception e){
            System.out.println("Socket was not set: " + e.getMessage());
        }
    }

    @Override
    public Operations prepare(BookingData bookingData, UUID transaktionId) {
        DatabaseConnection dbConn = new DatabaseConnection();
        LocalDate startDate = bookingData.getStartDate();
        LocalDate endDate = bookingData.getEndDate();
        int requestedId = bookingData.getSelectedRoom();

        try(Connection con = dbConn.getConn()) {

            //Check if there is already any Booking over this Item
            PreparedStatement stm = con.prepareStatement("SELECT * FROM booking");
            ResultSet rs = stm.executeQuery();

            while (rs.next()) {
                if(rs.getInt("roomId") == requestedId){
                    LocalDate startDateEntry = LocalDate.parse(rs.getString("startDate"));
                    LocalDate endDateEntry = LocalDate.parse(rs.getString("endDate"));
                    if (!isAvailable(startDate, endDate, startDateEntry, endDateEntry)) {
                        return Operations.ABORT;
                    }

                }
            }
            //Room is available
            String query = "INSERT INTO booking VALUES (\"" + transaktionId + "\", \"" + startDate + "\", \"" + endDate + "\", 0, " + requestedId + ")";
            System.out.println(query);
            stm = con.prepareStatement(query);
            stm.executeUpdate();
            System.out.println("successfully booked");

            return Operations.READY;
        }catch (Exception e){
            System.out.println("kapput");
            return Operations.ABORT;
        }

    }

    @Override
    public boolean commit(UUID transaktionId){
        DatabaseConnection dbConn = new DatabaseConnection();
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("UPDATE booking SET stable = 1 WHERE bookingID = \"" + transaktionId + "\"");
            System.out.println("UPDATE booking SET stable = 1 WHERE id = \"" + transaktionId + "\"");
            stm.executeUpdate();
            return true;
        }catch(Exception e){
            System.out.println("des kann jetzt nicht Wahrsteiner im Commit?: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean abort(UUID transaktionId){
        DatabaseConnection dbConn = new DatabaseConnection();
        try(Connection con = dbConn.getConn()){
            String s = "DELETE FROM booking WHERE bookingID = \"" + transaktionId + "\"";
            System.out.println(s);
            PreparedStatement stm = con.prepareStatement(s);
            stm.executeUpdate();
            return true;
        }catch(Exception e){
            System.out.println("des kann jetzt nicht Wahrsteiner im Abort?");
            return false;
        }
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
                if (!isAvailable(startDate, endDate, startDateEntry, endDateEntry)) {
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

            stm = con.prepareStatement("SELECT * FROM room WHERE roomID NOT IN (?)");
            stm.setString(1, availableRoomsIds);
            rs = stm.executeQuery();
            ArrayList<Object> availableRooms = new ArrayList<>();
            while(rs.next()){
                availableRooms.add(new Room(rs.getInt("roomID"), rs.getString("name")));
            }

            return availableRooms;

        }catch(Exception e){
            System.out.println("An Error has occured: " + e.getMessage());
            return null;
        }
    }

    public boolean isAvailable(LocalDate startDate, LocalDate endDate, LocalDate startDateEntry, LocalDate endDateEntry){
        if (startDateEntry.isAfter(endDate) | endDateEntry.isBefore(startDate)) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {

        }
}

