package cs451;

import java.io.IOException;
import java.io.Serializable;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class PerfectLink
{
	private static class Message implements Serializable
	{
//		private final InetAddress senderAddress;
		private final int senderRecvPort;
		private final String data;
		
//		public InetAddress getSenderAddress()
//		{
//			return senderAddress;
//		}
		
		public int getSenderRecvPort()
		{
			return senderRecvPort;
		}
		
		public String getData()
		{
			return data;
		}
		
		public Message(/*InetAddress senderAddress, */int senderRecvPort, String data)
		{
//			this.senderAddress = senderAddress;
			this.senderRecvPort = senderRecvPort;
			this.data = data;
		}
		
		static char STX = (char) 2; // start of text
		static char RS = (char) 30; // record separator
//		static char STX = (char) '!'; // start of text
//		static char RS = (char) '@'; // record separator
		
		public static Message fromBytes(byte[] bytes) throws RuntimeException, UnknownHostException
		{
			String string = new String(bytes, StandardCharsets.US_ASCII);
//			System.out.printf("fromBytes_string: %s%n", string);
			String[] parts = string.split(String.valueOf(RS));
			if (parts.length != 3 || !parts[0].equals(String.valueOf(STX)))
			{
//				System.out.printf("fromBytes_parts[0]: %s%n", parts[0]);
//				System.out.printf("fromBytes_parts[2]: %s%n", parts[2]);
//				System.out.printf("fromBytes_parts[1]: %s%n", parts[1]);
				throw new RuntimeException("Byte array is not a serialization of a Message object");
			}
			
//			return new Message(InetAddress.getByName(parts[1]),
//			                   Integer.parseInt(parts[2]),
//			                   parts[3]);
			return new Message(Integer.parseInt(parts[1]), parts[2]);
		}
		
		public byte[] getBytes()
		{
//			System.out.printf("STX: %c%n", STX);
//			System.out.printf("STX: %c%n", RS);
//			System.out.printf("STX: %d%n", senderRecvPort);
//			System.out.printf("STX: %s%n", data);
//			System.out.printf("String.format: %c%c%d%c%s%n", STX, RS, senderRecvPort, RS, data);
//			String fmt = (STX +
//					//RS +
//					//senderAddress.getHostAddress() +
//					RS +
//					senderRecvPort +
//					RS +
//					data);
			String fmt = String.format("%c%c%d%c%s", STX, RS, senderRecvPort, RS, data);
//			System.out.printf("getBytes_fmt: %s%n", fmt);
			return fmt.getBytes(StandardCharsets.US_ASCII);
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
		
		Message message = new Message(/*recvDS.getInetAddress(), */recvPort, data);
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
		Message ackMessage = new Message(/*recvDS.getInetAddress(), */recvPort, ackString);
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
