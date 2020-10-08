package cs451.PerfectLink;

import java.io.IOException;
import java.net.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PerfectLink
{
	private final int recvPort;
	private final DatagramSocket sendDS;
	private final DatagramSocket recvDS;
	
	private final AtomicInteger seqNum = new AtomicInteger(0);
	private final Set<Long> ackedSet = ConcurrentHashMap.newKeySet();
	
	private Thread sendThread;
	private Thread recvThread;
	
	public PerfectLink(int recvPort) throws SocketException
	{
		this.recvPort = recvPort;
		sendDS = new DatagramSocket();
		recvDS = new DatagramSocket(recvPort);
		sendThread = null;
		recvThread = null;
	}
	
	// ==================================================================================================== //
	
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
			
			long seq = seqNum.getAndIncrement();
			
			while (!ackedSet.contains(seq))
			{
				PLMessage plMessage = new PLMessage(PLMessage.PLMessageType.Normal, seq, recvPort, data);
				byte[] dataBytes = plMessage.getBytes();
				DatagramPacket dataDP = new DatagramPacket(dataBytes, dataBytes.length, address, port);
				try {
					sendDS.send(dataDP);
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				// TODO exponential backoff
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				System.out.printf("Sent: %s%n", plMessage.getData());
			}
		}
	}
	
	private class ReceiveThread implements Runnable
	{
		public void run()
		{
			while (!Thread.interrupted())
			{
				System.out.printf("Listening to port :%s%n", recvPort);
				
				// Receive datagram
				byte[] recvBuffer = new byte[1024];
				DatagramPacket dataDP = new DatagramPacket(recvBuffer, recvBuffer.length);
				try {
					recvDS.receive(dataDP);
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				// Process according to the type
				PLMessage recvPLMessage = PLMessage.fromBytes(recvBuffer);
				if (recvPLMessage.getMessageType() == PLMessage.PLMessageType.ACK)
				{
					System.out.printf("Received ACK from %s%n", dataDP.getAddress());
					ackedSet.add(recvPLMessage.getSeqNum());
				}
				else
				{
					System.out.printf("Received: %s%n", recvPLMessage.getData());
					System.out.printf("ACKing to %s:%d%n", dataDP.getAddress(), recvPLMessage.getSenderRecvPort());
					
					// ACK
					PLMessage ackPLMessage = new PLMessage(PLMessage.PLMessageType.ACK, recvPLMessage.getSeqNum(), recvPort, null);
					byte[] ackBytes = ackPLMessage.getBytes();
					DatagramPacket ackDP = new DatagramPacket(ackBytes, ackBytes.length, dataDP.getAddress(), recvPLMessage.getSenderRecvPort());
					try {
						sendDS.send(ackDP);
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					System.out.println("ACK sent");
					
					// TODO ...
				}
			}
		}
	}
	
	// ==================================================================================================== //
	
	public void send(String data, InetAddress address, int port)
	{
		sendThread = new Thread(new SendThread(data, address, port));
		sendThread.start();
	}
	
	public void startReceiving(/* TODO callback */)
	{
		recvThread = new Thread(new ReceiveThread());
		recvThread.start();
	}
	
	// TODO use
	public void close()
	{
		if (sendThread != null)
			sendThread.interrupt();
		if (recvThread != null)
			recvThread.interrupt();
		sendDS.close();
		recvDS.close();
	}
}
