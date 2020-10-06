package cs451;

import java.io.IOException;
import java.io.Serializable;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class PerfectLink
{
	private static class Message implements Serializable
	{
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
		
		public Message(int senderRecvPort, String data)
		{
			this.senderRecvPort = senderRecvPort;
			this.data = data;
		}
		
		static char STX = (char) 2; // start of text
		static char RS = (char) 30; // record separator
		
		public static Message fromBytes(byte[] bytes) throws RuntimeException
		{
			String string = new String(bytes, StandardCharsets.US_ASCII);
			String[] parts = string.split(String.valueOf(RS));
			if (parts.length != 3 || !parts[0].equals(String.valueOf(STX)))
			{
				throw new RuntimeException("Byte array is not a serialization of a Message object");
			}
			
			return new Message(Integer.parseInt(parts[1]), parts[2]);
		}
		
		public byte[] getBytes()
		{
			return String.format("%c%c%d%c%s", STX, RS, senderRecvPort, RS, data).getBytes(StandardCharsets.US_ASCII);
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
	
//	private class SendThread implements Runnable
//	{
//		String data;
//
//		public SendThread(String data)
//		{
//			this.data = data;
//		}
//
//		public void run()
//		{
//			// TODO
//		}
//	}
	// Receive thread has its own ackSendDS
	
	public void send(String data, InetAddress address, int port) throws IOException
	{
//		new Thread(new SendThread(data)).start();
		
		System.out.printf("Send to %s:%d%n", address, port);
		
		Message message = new Message(recvPort, data);
		byte[] dataBytes = message.getBytes();
		DatagramPacket dataDP = new DatagramPacket(dataBytes, dataBytes.length, address, port);
		sendDS.send(dataDP);
		
		System.out.printf("Sent: %s%n", message.getData());
		
		// Wait ACK
		byte[] ackBuffer = new byte[256];
		DatagramPacket ackDP = new DatagramPacket(ackBuffer, ackBuffer.length);
		recvDS.receive(ackDP);
		Message ackMessage = Message.fromBytes(ackBuffer);
		if (ackMessage.getData().equals(String.format("%c%s", ACK, data)))
			System.out.println("ACK");
		
		System.out.println("ACK received");
	}
	
	public void receive() throws IOException
	{
		System.out.printf("Receive from :%s%n", recvPort);
		
		byte[] recvBuffer = new byte[256];
		DatagramPacket dataDP = new DatagramPacket(recvBuffer, recvBuffer.length);
		recvDS.receive(dataDP);
		Message recvMessage = Message.fromBytes(recvBuffer);
		
		System.out.printf("Received: %s%n", recvMessage.getData());
		System.out.printf("ACKing to %s:%d%n", dataDP.getAddress(), recvMessage.getSenderRecvPort());
		
		// ACK
		String ackString = String.format("%c%s", ACK, recvMessage.getData());
		Message ackMessage = new Message(recvPort, ackString);
		byte[] ackBytes = ackMessage.getBytes();
		DatagramPacket ackDP = new DatagramPacket(ackBytes, ackBytes.length, dataDP.getAddress(), recvMessage.getSenderRecvPort());
		sendDS.send(ackDP);
	}
	
	public void close()
	{
		sendDS.close();
		recvDS.close();
	}
}