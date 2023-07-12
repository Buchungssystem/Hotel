package org.hotel;

import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.utils.*;

import javax.xml.crypto.Data;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.UUID;

public class Execute {
    public static void main(String[] args) {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        Hotel h = new Hotel();
        LocalDate startDate = LocalDate.of(2023, 8, 1);
        LocalDate endDate = LocalDate.of(2023, 8, 14);
        h.getAvailableItems(startDate, endDate);

        while (true) {
            try (DatagramSocket dgSocket = new DatagramSocket(Participant.hotelPort)) {
                //byte data of UDP message
                byte[] messageData;

                //byte UDP Message
                byte [] parsedMessage;
                byte[] buffer = new byte[65507];
                //DatagramPacket for recieving data
                DatagramPacket dgPacketIn = new DatagramPacket(buffer, buffer.length);
                //response UDPMessage
                UDPMessage responeseMessage;

                System.out.println("Listening on Port " + Participant.hotelPort);
                dgSocket.receive(dgPacketIn);
                System.out.println("recieved aber trd im arsch: ");

                //string data to parse it into Objects
                String data = new String(dgPacketIn.getData(), 0, dgPacketIn.getLength());
                //parsed UDP message
                UDPMessage dataObject = objectMapper.readValue(data, UDPMessage.class);
                System.out.println(dataObject.getOperation());

                switch (dataObject.getOperation()){
                    case PREPARE -> {
                        System.out.println("prepare recieved");
                        messageData = dataObject.getData();
                        data = new String(messageData, 0, messageData.length);
                        BookingData bookingData = objectMapper.readValue(data, BookingData.class);

                        Operations response = h.prepare(bookingData, dataObject.getTransaktionNumber());
                        responeseMessage = new UDPMessage(dataObject.getTransaktionNumber(), new byte[0], SendingInformation.HOTEL, response);

                        //send response back to TravelBroker
                        messageData = objectMapper.writeValueAsBytes(responeseMessage);
                        DatagramPacket dgOutPrepare = new DatagramPacket(messageData, messageData.length, Participant.localhost, Participant.travelBrokerPort);

                        dgSocket.send(dgOutPrepare);
                        System.out.println("prepare answered: " + response);
                    }
                    case COMMIT -> {
                        System.out.println("commit recieved");
                        if(h.commit(dataObject.getTransaktionNumber())) {
                            responeseMessage = new UDPMessage(dataObject.getTransaktionNumber(), new byte[0], SendingInformation.HOTEL, Operations.OK);
                            parsedMessage = objectMapper.writeValueAsBytes(responeseMessage);
                            DatagramPacket dgOutCommit = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, Participant.travelBrokerPort);

                            dgSocket.send(dgOutCommit);
                            System.out.println("commit answered - Ok");
                        }
                    }
                    case ABORT -> {
                        System.out.println("abort recieved");
                        if(h.abort(dataObject.getTransaktionNumber())){
                            responeseMessage = new UDPMessage(dataObject.getTransaktionNumber(), new byte[0], SendingInformation.HOTEL, Operations.OK);
                            parsedMessage = objectMapper.writeValueAsBytes(responeseMessage);
                            DatagramPacket dgOutAbort = new DatagramPacket(parsedMessage, parsedMessage.length, Participant.localhost, Participant.travelBrokerPort);

                            dgSocket.send(dgOutAbort);
                            System.out.println("abort answered - OK");
                        }
                    }
                    case AVAILIBILITY -> {
                        messageData = dataObject.getData();
                        AvailabilityData availabilityData = objectMapper.readValue(messageData, AvailabilityData.class);

                        ArrayList<Object> availableItems = h.getAvailableItems(availabilityData.getStartDate(), availabilityData.getEndDate());
                        System.out.println(availableItems.get(0));
                        if(!(availableItems == null)){
                            byte[] parsedItems = objectMapper.writeValueAsBytes(availableItems);
                            UDPMessage responseMessage = new UDPMessage(dataObject.getTransaktionNumber(), parsedItems, SendingInformation.HOTEL, Operations.AVAILIBILITY);
                            parsedMessage = objectMapper.writeValueAsBytes(responseMessage);
                            //Datagrampacket for sending the response
                            DatagramPacket dgOutAvailability= new DatagramPacket(parsedMessage, parsedMessage.length, h.localhost, h.travelBrokerPort);
                            dgSocket.send(dgOutAvailability);

                        }
                    }
                }

            } catch (Exception e) {

            }
        }
    }
}

