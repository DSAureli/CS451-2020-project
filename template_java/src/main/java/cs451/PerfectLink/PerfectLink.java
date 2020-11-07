package cs451.PerfectLink;

import java.io.IOException;
import java.io.NotSerializableException;
import java.net.*;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class PerfectLink
{
	private final int recvPort;
	private final DatagramSocket sendDS;
	private final DatagramSocket recvDS;
	
	public PerfectLink(int recvPort) throws SocketException
	{
		this.recvPort = recvPort;
		sendDS = new DatagramSocket();
		recvDS = new DatagramSocket(recvPort);
	}
	
	private final AtomicInteger seqNum = new AtomicInteger(0);
	private final Map<Long, Boolean> ackedMap = new ConcurrentHashMap<>(); // <sequence number, ack received>
	private final Set<AbstractMap.SimpleEntry<Integer, Long>> receivedSet = ConcurrentHashMap.newKeySet(); // <sender port, sequence number>
	// TODO discard messages sent too far in the past, so that the set can be "pruned" at fixed intervals
	//     (send timestamp together with messages)
	
	private Thread sendThread = null;
	private Thread recvThread = null;
	
	// ==================================================================================================== //
	
	private class SendThread implements Runnable
	{
		String data;
		InetAddress address;
		int port;

		public SendThread(InetAddress address, int port, String data)
		{
			this.address = address;
			this.port = port;
			this.data = data;
		}

		public void run()
		{
			System.out.printf("Sending %s to :%d%n", data, port);
			
			long seq = seqNum.getAndIncrement();
			ackedMap.put(seq, false);
			
			while (!ackedMap.get(seq))
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
			}
			
			System.out.printf("Sent: %s%n", data);
			
			// entry in ackedMap for this seq is not needed anymore
			ackedMap.remove(seq);
		}
	}
	
	private class ReceiveThread implements Runnable
	{
		Consumer<String> callback;
		
		public ReceiveThread(Consumer<String> callback)
		{
			this.callback = callback;
		}
		
		public void run()
		{
			while (!Thread.interrupted())
			{
				// Receive datagram
				byte[] recvBuffer = new byte[1024];
				DatagramPacket dataDP = new DatagramPacket(recvBuffer, recvBuffer.length);
				try {
					recvDS.receive(dataDP);
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				// Deserialize message
				PLMessage recvPLMessage;
				try
				{
					recvPLMessage = PLMessage.fromBytes(recvBuffer);
				}
				catch (NotSerializableException e)
				{
					e.printStackTrace();
					return;
				}
				
				// Process according to the type
				if (recvPLMessage.getMessageType() == PLMessage.PLMessageType.ACK)
				{
					// ACK
//					System.out.printf("Received ACK from %s%n", dataDP.getAddress());
					
					// Mark sequence number as acked if it's present in the map
					ackedMap.replace(recvPLMessage.getSeqNum(), true);
				}
				else
				{
					// Normal
					// Send ACK
					PLMessage ackPLMessage = new PLMessage(PLMessage.PLMessageType.ACK, recvPLMessage.getSeqNum(), recvPort, null);
					byte[] ackBytes = ackPLMessage.getBytes();
					DatagramPacket ackDP = new DatagramPacket(ackBytes, ackBytes.length, dataDP.getAddress(), recvPLMessage.getSenderRecvPort());
					try {
						sendDS.send(ackDP);
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					AbstractMap.SimpleEntry<Integer, Long> receivedSetEntry =
							new AbstractMap.SimpleEntry<>(recvPLMessage.getSenderRecvPort(), recvPLMessage.getSeqNum());
					boolean alreadyReceived;
					// test&set
					synchronized (this)
					{
						alreadyReceived = receivedSet.contains(receivedSetEntry);
						if (!alreadyReceived)
							receivedSet.add(receivedSetEntry);
					}

					if (!alreadyReceived)
						callback.accept(recvPLMessage.getData());
				}
			}
		}
	}
	
	// ==================================================================================================== //
	
	public void send(InetAddress address, int port, String data)
	{
		sendThread = new Thread(new SendThread(address, port, data));
		sendThread.start();
	}
	
	public void startReceiving(Consumer<String> callback)
	{
		recvThread = new Thread(new ReceiveThread(callback));
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
