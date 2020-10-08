package cs451;

import java.io.IOException;
import java.io.Serializable;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class PerfectLink
{
	// ASCII Control Codes
	private static class CC
	{
		public static char SOH = (char) 1; // start of heading
		public static char STX = (char) 2; // start of text
		public static char ENQ = (char) 5; // enquiry
		public static char ACK = (char) 6; // acknowledge
		public static char RS = (char) 30; // record separator
	}
	
	// TODO I'll need to readd the address of the original sender to Message, as it would be useful for ACK to the original process
	// TODO Plus, maybe keeping an int for reordering in Message could work
	private static class PLMessage implements Serializable
	{
		public enum PLMessageType
		{
			Normal, ACK
		}
		
		private final PLMessageType messageType;
		private final int senderRecvPort;
		private final String data;
		
		public int getSenderRecvPort()
		{
			return senderRecvPort;
		}
		
		public String getData()
		{
			return data;
		}
		
		public PLMessage(PLMessageType messageType, int senderRecvPort, String data)
		{
			this.messageType = messageType;
			this.senderRecvPort = senderRecvPort;
			this.data = data;
		}
		
		private static void error(String error) throws RuntimeException
		{
			throw new RuntimeException("Byte array is not a serialization of a Message object: " + error);
		}
		
		public static PLMessage fromBytes(byte[] bytes) throws RuntimeException
		{
			String string = new String(bytes, StandardCharsets.US_ASCII);
			String[] parts = string.split(String.valueOf(CC.STX), 2);
			if (parts.length < 2)
				error("missing STX");
			
			String[] headerParts = parts[0].split(String.valueOf(CC.RS));
			if (headerParts.length != 3 || !headerParts[0].equals(String.valueOf(CC.SOH)))
				error("malformed header");
			
			PLMessageType type = headerParts[1].equals(String.valueOf(CC.ENQ)) ? PLMessageType.Normal : PLMessageType.ACK;
			return new PLMessage(type, Integer.parseInt(headerParts[2]), parts[1]);
		}
		
		public byte[] getBytes()
		{
			char type = messageType == PLMessageType.Normal ? CC.ENQ : CC.ACK;
			return String.format("%c%c%c%c%d%c%s", CC.SOH, CC.RS, type, CC.RS, senderRecvPort, CC.STX, data)
					.getBytes(StandardCharsets.US_ASCII);
		}
	}
	
	private final int recvPort;
	private final DatagramSocket sendDS;
	private final DatagramSocket recvDS;
	
	static char ACK = (char) 6;
	
	public PerfectLink(int recvPort) throws SocketException
	{
		this.recvPort = recvPort;
		
		sendDS = new DatagramSocket();
		recvDS = new DatagramSocket(recvPort);
	}
	
	private class SendThread implements Runnable
	{
		String data;
		InetAddress address;
		int port;

		public SendThread(String data, InetAddress address, int port)
		{
			this.data = data;
			this.address = address;
			this.port = port;
		}

		public void run()
		{
			System.out.printf("Send to %s:%d%n", address, port);
			
			PLMessage plMessage = new PLMessage(PLMessage.PLMessageType.Normal, recvPort, data);
			byte[] dataBytes = plMessage.getBytes();
			DatagramPacket dataDP = new DatagramPacket(dataBytes, dataBytes.length, address, port);
			try {
				sendDS.send(dataDP);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			System.out.printf("Sent: %s%n", plMessage.getData());
			
			// Wait ACK
			byte[] ackBuffer = new byte[256];
			DatagramPacket ackDP = new DatagramPacket(ackBuffer, ackBuffer.length);
			try {
				recvDS.receive(ackDP);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			PLMessage ackPLMessage = PLMessage.fromBytes(ackBuffer);
			if (ackPLMessage.getData().equals(String.format("%c%s", ACK, data)))
				System.out.println("ACK");
			
			System.out.println("ACK received");
		}
	}
	
	private class ReceiveThread implements Runnable
	{
		// TODO  Receive thread has its own ackSendDS?
		
		public ReceiveThread() { }
		
		public void run()
		{
				System.out.printf("Receive from :%s%n", recvPort);
				
				byte[] recvBuffer = new byte[256];
				DatagramPacket dataDP = new DatagramPacket(recvBuffer, recvBuffer.length);
				try {
					recvDS.receive(dataDP);
				} catch (IOException e) {
					e.printStackTrace();
				}
				PLMessage recvPLMessage = PLMessage.fromBytes(recvBuffer);
				
				System.out.printf("Received: %s%n", recvPLMessage.getData());
				System.out.printf("ACKing to %s:%d%n", dataDP.getAddress(), recvPLMessage.getSenderRecvPort());
				
				// ACK
				String ackString = String.format("%c%s", ACK, recvPLMessage.getData());
				PLMessage ackPLMessage = new PLMessage(PLMessage.PLMessageType.ACK, recvPort, ackString);
				byte[] ackBytes = ackPLMessage.getBytes();
				DatagramPacket ackDP = new DatagramPacket(ackBytes, ackBytes.length, dataDP.getAddress(), recvPLMessage.getSenderRecvPort());
				try {
					sendDS.send(ackDP);
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				System.out.println("ACK sent");
		}
	}
	
	public void send(String data, InetAddress address, int port)
	{
		new Thread(new SendThread(data, address, port)).start();
	}
	
	public void receive()
	{
		new Thread(new ReceiveThread()).start();
	}
	
	public void close()
	{
		// TODO use
		sendDS.close();
		recvDS.close();
	}
}
