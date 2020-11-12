package cs451.PerfectLink;

import java.io.IOException;
import java.io.NotSerializableException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class PerfectLink
{
	private final int plRecvPort;
	private final DatagramSocket sendDS;
	private final DatagramSocket recvDS;
	
	private final ExecutorService recvThreadPool;
	private final ExecutorService sendThreadPool;
	
	private final Thread sendCoordinatorThread;
	private final Thread recvCoordinatorThread;
	
	public PerfectLink(int recvPort, Consumer<String> callback, ExecutorService recvThreadPool, ExecutorService sendThreadPool) throws SocketException
	{
		this.plRecvPort = recvPort;
		this.sendDS = new DatagramSocket();
		this.recvDS = new DatagramSocket(recvPort);
		
		this.recvThreadPool = recvThreadPool;
		this.sendThreadPool = sendThreadPool;
		
		this.sendCoordinatorThread = new Thread(new SendCoordinatorThread());
		this.sendCoordinatorThread.start();
		
		this.recvCoordinatorThread = new Thread(new ReceiveCoordinatorThread(callback));
		this.recvCoordinatorThread.start();
	}
	
	private final AtomicInteger nextSeqNum = new AtomicInteger(0);
	private final Set<Long> waitingACKSet = ConcurrentHashMap.newKeySet();
	private final Set<Pair<Integer, Long>> receivedSet = ConcurrentHashMap.newKeySet(); // <sender port, sequence number>
	// TODO discard messages sent too far in the past, so that the set can be "pruned" at fixed intervals
	//      (send timestamp together with messages)
	
	// Messages still to be sent/acked
	private final PriorityBlockingQueue<PLRequest> pendingSendQueue = new PriorityBlockingQueue<>(1, Comparator.comparingLong(PLRequest::getTimestamp));
	
	final Lock sendCoordinatorLock = new ReentrantLock();
	final Condition sendCoordinatorCondition = sendCoordinatorLock.newCondition();
	
	private void addRequestToSendQueue(PLRequest request)
	{
		sendCoordinatorLock.lock();
		pendingSendQueue.add(request);
		sendCoordinatorCondition.signal();
		sendCoordinatorLock.unlock();
	}
	
	// ============================================================================================================== //
	
	private class SendCoordinatorThread implements Runnable
	{
		public void run()
		{
			while (!Thread.interrupted())
			{
				// Get the earliest request
				PLRequest request;
				try
				{
					// take() is blocking
					request = pendingSendQueue.take();
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
					Thread.currentThread().interrupt();
					return;
				}
				
				if (request.getTimestamp() <= System.currentTimeMillis())
				{
					sendThreadPool.submit(new SendThread(request));
					
					// We always want to send ACKs and only once, so we don't re-insert it in the queue
					if (request.getType() == PLMessage.PLMessageType.Normal && waitingACKSet.contains(request.getSeqNum()))
					{
						// Message hasn't been ACK'd yet, re-insert request in the queue with delay
						// TODO exponential backoff
						pendingSendQueue.add(new PLRequest(request, request.getTimestamp() + 10));
					}
				}
				else
				{
					try
					{
						// Acquire lock for the Condition
						sendCoordinatorLock.lock();
						
						// Re-insert request
						pendingSendQueue.add(request);
						
						// Wait for request timestamp or queue insertion signal
						// await releases the lock!
						sendCoordinatorCondition.awaitUntil(new Date(request.getTimestamp()));
						
						// !!! AWAIT DOES NOT ALWAYS RELEASE THE LOCK YOU FUCKING JAVA !!!
						sendCoordinatorLock.unlock();
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
						Thread.currentThread().interrupt();
						return;
					}
				}
			}
		}
	}
	
	private class SendThread implements Runnable
	{
		private final PLRequest request;
		
		public SendThread(PLRequest request)
		{
			this.request = request;
		}

		public void run()
		{
//			System.out.printf("Sending %s to :%d%n", request.toPLMessage(plRecvPort), request.getPort());
			
			byte[] plMessageBytes = request.toPLMessage(plRecvPort).getBytes();
			DatagramPacket plMessageDP = new DatagramPacket(plMessageBytes, plMessageBytes.length, request.getAddress(), request.getPort());
			try {
				sendDS.send(plMessageDP);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private class ReceiveCoordinatorThread implements Runnable
	{
		private final Consumer<String> callback;
		
		public ReceiveCoordinatorThread(Consumer<String> callback)
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
					continue;
				}
				
				recvThreadPool.submit(new ReceiveThread(callback, dataDP.getAddress(), recvBuffer));
			}
		}
	}
	
	private class ReceiveThread implements Runnable
	{
		private final Consumer<String> callback;
		private final InetAddress senderAddress;
		private final byte[] recvBuffer;
		
		public ReceiveThread(Consumer<String> callback, InetAddress senderAddress, byte[] recvBuffer)
		{
			this.callback = callback;
			this.senderAddress = senderAddress;
			this.recvBuffer = recvBuffer;
		}
		
		public void run()
		{
			// Deserialize message
			PLMessage recvPLMessage;
			try
			{
				recvPLMessage = PLMessage.fromBytes(recvBuffer);
			}
			catch (NotSerializableException e)
			{
				e.printStackTrace();
				Thread.currentThread().interrupt();
				return;
			}
			
//			System.out.printf("Received %s from :%d%n", recvPLMessage, recvPLMessage.getSenderRecvPort());
			
			// Process according to the type
			if (recvPLMessage.getMessageType() == PLMessage.PLMessageType.ACK)
			{
				waitingACKSet.remove(recvPLMessage.getSeqNum());
			}
			else // Normal
			{
				// Send ACK
				addRequestToSendQueue(PLRequest.newACKRequest(System.currentTimeMillis(), recvPLMessage.getSeqNum(), senderAddress, recvPLMessage.getSenderRecvPort()));
				
				Pair<Integer, Long> receivedSetEntry = new Pair<>(recvPLMessage.getSenderRecvPort(), recvPLMessage.getSeqNum());
				boolean alreadyReceived;
				// test&set
				synchronized (PerfectLink.class)
				{
					alreadyReceived = receivedSet.contains(receivedSetEntry);
					if (!alreadyReceived)
						receivedSet.add(receivedSetEntry);
				}
				
				if (!alreadyReceived)
					recvThreadPool.submit(() -> callback.accept(recvPLMessage.getData()));
			}
		}
	}
	
	// ============================================================================================================== //
	
	public void send(InetAddress address, int port, String data)
	{
		long seqNum = nextSeqNum.getAndIncrement();
		waitingACKSet.add(seqNum);
		addRequestToSendQueue(PLRequest.newNormalRequest(System.currentTimeMillis(), seqNum, address, port, data));
	}
	
	public void close()
	{
		sendCoordinatorThread.interrupt();
		recvCoordinatorThread.interrupt();
		
		recvThreadPool.shutdownNow();
		sendThreadPool.shutdownNow();
		
		sendDS.close();
		recvDS.close();
	}
}
