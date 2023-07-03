package org.hotel;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.utils.AvailabilityData;
import org.utils.Operations;
import org.utils.UDPMessage;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.UUID;

public class Execute {
    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();
        Hotel h = new Hotel();
        LocalDate startDate = LocalDate.of(2023, 8, 1);
        LocalDate endDate = LocalDate.of(2023, 8, 14);
        h.getAvailableItems(startDate, endDate, UUID.randomUUID());
        while (true) {
            try (DatagramSocket dgSocket = new DatagramSocket(4445)) {
                byte[] buffer = new byte[65507];
                //DatagramPacket for recieving data
                DatagramPacket dgPacketIn = new DatagramPacket(buffer, buffer.length);

                System.out.println("Listening on Port 4445..");
                dgSocket.receive(dgPacketIn);
                String data = new String(dgPacketIn.getData(), 0, dgPacketIn.getLength());
                UDPMessage dataObject = objectMapper.readValue(data, UDPMessage.class);

                switch (dataObject.getOperation()){
                    case PREPARE -> {

                    }
                    case COMMIT -> {

                    }
                    case ABORT -> {

                    }
                    case READY -> {

                    }
                    case AVAILIBILITY -> {
                        byte[] messageData = dataObject.getData();
                        AvailabilityData availabilityData = objectMapper.readValue(messageData, AvailabilityData.class);

                        byte [] response = h.getAvailableItems(availabilityData.getStartDate(), availabilityData.getEndDate(), dataObject.getTransaktionNumber());
                        //Datagrampacket for sending the response
                        DatagramPacket dgPacketOut = new DatagramPacket(response, response.length, h.localhost, h.travelBrokerPort);
                    }
                }

            } catch (Exception e) {

            }
        }
    }
}

