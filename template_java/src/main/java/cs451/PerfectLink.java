package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;

public class PerfectLink
{
	private static class Message implements Serializable
	{
		private final InetAddress senderAddress;
		private final int senderRecvPort;
		private final String data;
		
		public InetAddress getSenderAddress()
		{
			return senderAddress;
		}
		
		public int getSenderRecvPort()
		{
			return senderRecvPort;
		}
		
		public String getData()
		{
			return data;
		}
		
		public Message(InetAddress senderAddress, int senderRecvPort, String data)
		{
			this.senderAddress = senderAddress;
			this.senderRecvPort = senderRecvPort;
			this.data = data;
		}
		
		static char start = (char) 2; // STX (start of text)
		static char sep = (char) 30; // RS (record separator)
		
		public static Message fromBytes(byte[] bytes) throws RuntimeException, UnknownHostException
		{
			String[] parts = new String(bytes, StandardCharsets.US_ASCII).split(String.valueOf(sep));
			if (parts.length != 4 || !parts[0].equals(String.valueOf(start)))
			{
				throw new RuntimeException("Byte array is not a serialization of a Message object");
			}
			
			return new Message(InetAddress.getByName(parts[1]),
			                   Integer.parseInt(parts[2]),
			                   parts[3]);
		}
		
		public byte[] getBytes()
		{
			return new String(start +
					                  sep +
					                  senderAddress.getHostAddress() +
					                  sep +
					                  senderRecvPort +
					                  sep +
					                  data)
					.getBytes(StandardCharsets.US_ASCII);
		}
	}
	
	DatagramSocket sendDS;
	DatagramSocket recvDS;
	
	public PerfectLink(int recvPort, int destPort) throws SocketException
	{
		this.recvPort = recvPort;
		this.destPort = destPort;
		
		sendDS = new DatagramSocket();
		recvDS = new DatagramSocket(recvPort);
	}
	
	public void send(String data, InetAddress address, int port) throws IOException
	{
		System.out.printf("Send to %s:%d%n", address, port);
		byte[] dataBytes = data.getBytes();
		DatagramPacket dataDP = new DatagramPacket(dataBytes, dataBytes.length, address, port);
		sendDS.send(dataDP);
		
		System.out.println("Sent");
		
		// Wait ACK
		byte[] ackBuffer = new byte[256];
		DatagramPacket ackDP = new DatagramPacket(ackBuffer, ackBuffer.length);
		recvDS.receive(ackDP);
		String ackString = new String(ackBuffer, StandardCharsets.UTF_8);
		if (ackString.equals(String.format("ACK %s", data)))
			System.out.println("ACK");
		
		System.out.println("ACK received");
	}
	
	public void receive() throws IOException
	{
		System.out.printf("Receive from :%s", recvPort);
		
		byte[] recvBuffer = new byte[256];
		DatagramPacket dataDP = new DatagramPacket(recvBuffer, recvBuffer.length);
		recvDS.receive(dataDP);
		String recvString = new String(recvBuffer, StandardCharsets.UTF_8);
		
		System.out.println("Received");
		System.out.printf("ACKing to %s:%d%n", dataDP.getAddress(), dataDP.getPort());
		
		// ACK
		String ackString = String.format("ACK %s", recvString);
		byte[] ackBytes = ackString.getBytes();
		DatagramPacket ackDP = new DatagramPacket(ackBytes, ackBytes.length, dataDP.getAddress(), destPort);
		sendDS.send(ackDP);
	}
	
	public void close()
	{
		sendDS.close();
		recvDS.close();
	}
}
