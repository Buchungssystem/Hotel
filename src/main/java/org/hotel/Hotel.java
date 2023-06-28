package org.hotel;

import org.utils.Participant;
import org.utils.VoteOptions;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.ArrayList;

public class Hotel extends Participant {

    @Override
    public VoteOptions Vote() {
        return VoteOptions.ABORT;
    }

    @Override
    public void book() {

    }

    @Override
    public void getAvailableItems(LocalDate startDate, LocalDate endDate) {
        DatabaseConnection dbConn = new DatabaseConnection();
        ArrayList<String> availableRooms = new ArrayList<>();
        ResultSet rs = null;
        try(Connection con = dbConn.getConn()){
            PreparedStatement stm = con.prepareStatement("SELECT * FROM booking");
            rs = stm.executeQuery();

            while(rs.next()){
                LocalDate startDateEntry = LocalDate.parse(rs.getString("startDate"));
                LocalDate endDateEntry = LocalDate.parse(rs.getString("endDate"));
                if (startDateEntry.isAfter(endDate) | endDateEntry.isBefore(startDate)) {
                    availableRooms.add(rs.getString("roomId"));
                }
            }
            String availableRoomsIds = "";
            for (int i = 0; i < availableRooms.size(); i++) {
                if( i < availableRooms.size() - 1){
                    availableRoomsIds += availableRooms.get(i) + ", ";
                }else{
                    availableRoomsIds += availableRooms.get(i);
                }
            }

            stm = con.prepareStatement("SELECT * FROM room WHERE id IN (?)");
            stm.setString(1, availableRoomsIds);
            rs = stm.executeQuery();

            while(rs.next()){
                System.out.println("ID:" + rs.getString("id") +
                                    "\nName: " + rs.getString("name"));
            }

        }catch(Exception e){
            System.out.println("Something went wrong with the DB-Query.");
        }


    }

    public static void main(String[] args) {
        Hotel h = new Hotel();
        LocalDate startDate = LocalDate.of(2023, 8, 1);
        LocalDate endDate = LocalDate.of(2023, 8, 14);
        h.getAvailableItems(startDate, endDate);
        /*while (true) {
            try (DatagramSocket dgSocket = new DatagramSocket(4445)) {
                byte[] buffer = new byte[65507];
                DatagramPacket dgPacket = new DatagramPacket(buffer, buffer.length);
                System.out.println("Listening on Port 4445..");
                dgSocket.receive(dgPacket);
                String data = new String(dgPacket.getData(), 0, dgPacket.getLength());
                System.out.println(data);
            } catch (Exception e) {

            }*/
        //}
    }
}
