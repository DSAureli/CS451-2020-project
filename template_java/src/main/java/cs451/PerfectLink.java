package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;

public class PerfectLink
{
//	DatagramSocket sendDS;
//	DatagramSocket recvDS;
//	DatagramSocket ackSendDS;
//	DatagramSocket ackRecvDS;
	int recvPort;
	int destPort;
	
	public PerfectLink(int recvPort, int destPort) throws SocketException
	{
//		sendDS = new DatagramSocket();
//		recvDS = new DatagramSocket(recvPort);
//		ackSendDS = new DatagramSocket(recvPort);
//		ackRecvDS = new DatagramSocket(recvPort);
		this.recvPort = recvPort;
		this.destPort = destPort;
	}
	
	public void send(String data, InetAddress address, int port) throws IOException
	{
		System.out.printf("Send to %s:%d%n", address, port);
		
		DatagramSocket sendDS = new DatagramSocket();
		byte[] dataBytes = data.getBytes();
		DatagramPacket dataDP = new DatagramPacket(dataBytes, dataBytes.length, address, port);
		sendDS.send(dataDP);
		sendDS.close();
		
		System.out.println("Sent");
		
		// Wait ACK
		DatagramSocket ackRecvDS = new DatagramSocket(recvPort);
		byte[] ackBuffer = new byte[256];
		DatagramPacket ackDP = new DatagramPacket(ackBuffer, ackBuffer.length);
		ackRecvDS.receive(ackDP);
		String ackString = new String(ackBuffer, StandardCharsets.UTF_8);
		if (ackString.equals(String.format("ACK %s", data)))
			System.out.println("ACK");
		ackRecvDS.close();
		
		System.out.println("ACK received");
	}
	
	public void receive() throws IOException
	{
		System.out.printf("Receive from :%s", recvPort);
		
		DatagramSocket recvDS = new DatagramSocket(recvPort);
		byte[] recvBuffer = new byte[256];
		DatagramPacket dataDP = new DatagramPacket(recvBuffer, recvBuffer.length);
		recvDS.receive(dataDP);
		String recvString = new String(recvBuffer, StandardCharsets.UTF_8);
		recvDS.close();
		
		System.out.println("Received");
		System.out.printf("ACKing to %s:%d%n", dataDP.getAddress(), dataDP.getPort());
		
		// ACK
		DatagramSocket ackSendDS = new DatagramSocket();
		String ackString = String.format("ACK %s", recvString);
		byte[] ackBytes = ackString.getBytes();
		DatagramPacket ackDP = new DatagramPacket(ackBytes, ackBytes.length, dataDP.getAddress(), destPort);
		ackSendDS.send(ackDP);
		ackSendDS.close();
	}
	
	public void close()
	{
//		sendDS.close();
//		recvDS.close();
//		ackSendDS.close();
//		ackRecvDS.close();
	}
}
