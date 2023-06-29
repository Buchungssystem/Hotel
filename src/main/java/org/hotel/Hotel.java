package org.hotel;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.utils.Operations;
import org.utils.Participant;
import org.utils.SendingInformation;
import org.utils.UDPMessage;
import org.utils.Operations;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
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
    public void book() {

    }

    @Override
    public byte[] getAvailableItems(LocalDate startDate, LocalDate endDate, UUID pTransaktionnumber) {
        DatabaseConnection dbConn = new DatabaseConnection();
        ArrayList<String> availableRoomIds = new ArrayList<>();
        ResultSet rs = null;
        ObjectMapper objectMapper = new ObjectMapper();
        UDPMessage udpMessage;
        byte[] data;
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("SELECT * FROM booking");
            rs = stm.executeQuery();

            while(rs.next()){
                LocalDate startDateEntry = LocalDate.parse(rs.getString("startDate"));
                LocalDate endDateEntry = LocalDate.parse(rs.getString("endDate"));
                if (startDateEntry.isAfter(endDate) | endDateEntry.isBefore(startDate)) {
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

            stm = con.prepareStatement("SELECT * FROM room WHERE id IN (?)");
            stm.setString(1, availableRoomsIds);
            rs = stm.executeQuery();
            ArrayList<Room> availableRooms = new ArrayList<>();
            while(rs.next()){
                availableRooms.add(new Room(rs.getInt("id"), rs.getString("name"), rs.getDouble("price")));
            }

            data = objectMapper.writeValueAsBytes(availableRooms);
            udpMessage = new UDPMessage(pTransaktionnumber, data, SendingInformation.HOTEL, Operations.AVAILIBILITY);

            return objectMapper.writeValueAsBytes(udpMessage);

        }catch(Exception e){
            String errorMessage = "Something went wrong:\n" + e.getMessage();
            byte[] res;
            try {
                data = objectMapper.writeValueAsBytes(errorMessage);
                udpMessage = new UDPMessage(pTransaktionnumber, data, SendingInformation.HOTEL, Operations.AVAILIBILITY);
                res = objectMapper.writeValueAsBytes(udpMessage);
            }catch (com.fasterxml.jackson.core.JsonProcessingException er){
                return errorMessage.getBytes();
            }

            return res;
        }
    }

    public static void main(String[] args) {

        }
}

