package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;

public class PerfectLink
{
	int recvPort;
	int destPort;
	
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
