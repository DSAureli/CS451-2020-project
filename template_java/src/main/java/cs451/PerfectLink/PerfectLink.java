package cs451.PerfectLink;

import cs451.Helper.Pair;

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
	
	//// Data for retransmission protocol [IETF RFC 6298] ////
	private final ConcurrentHashMap<Integer, RTOData> rtoDataMap;
	
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
		
		this.rtoDataMap = new ConcurrentHashMap<>();
	}
	
	private final AtomicInteger nextSeqNum = new AtomicInteger(0);
	private final Set<Long> waitingACKSet = ConcurrentHashMap.newKeySet();
	private final Set<Pair<Integer, Long>> receivedSet = ConcurrentHashMap.newKeySet(); // <sender port, sequence number>
	// TODO discard messages sent too far in the past, so that the set can be "pruned" at fixed intervals
	//      (send timestamp together with messages)
	
	// Messages/ACKs still to be sent
	private final PriorityBlockingQueue<PLRequest> pendingSendQueue = new PriorityBlockingQueue<>(1, Comparator.comparingLong(PLRequest::getSchedTimestamp));
	
	private final Lock sendCoordinatorLock = new ReentrantLock();
	private final Condition sendCoordinatorCondition = sendCoordinatorLock.newCondition();
	
	private void setRTOData(int port, boolean firstRTT, double SRTT, double RTTVAR, long RTO)
	{
		//// TCP's Retransmission Timer Algorithm [IETF RFC 6298] ////
		
		long minRTO = 500; // EDIT (original: 1000)
		long maxRTO = 10 * 1000; // EDIT (original: 60 * 1000)
		
		RTO = Math.min(maxRTO, Math.max(minRTO, RTO));
		
		////
		
		rtoDataMap.put(port, new RTOData(firstRTT, SRTT, RTTVAR, RTO));
	}
	
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
				
				if (request.getSchedTimestamp() <= System.currentTimeMillis())
				{
					sendThreadPool.submit(new SendThread(request));
					
					// We always want to send ACKs and only once, so we don't re-insert it in the queue
					if (request instanceof DataPLRequest)
					{
						if (waitingACKSet.contains(request.getSeqNum()))
						{
							// Message hasn't been ACK'd yet, "back off the timer"
							
							//// TCP's Retransmission Timer Algorithm [IETF RFC 6298] ////
							
							synchronized (PerfectLink.class)
							{
								RTOData rtoData = rtoDataMap.get(request.getPort());
								long newRTO = 2 * rtoData.getRTO();
								setRTOData(request.getPort(), rtoData.isFirstRTT(), rtoData.getSRTT(), rtoData.getRTTVAR(), newRTO);
							}
							
							////
							
							// Re-insert request in the queue with delay
							pendingSendQueue.add(
								new DataPLRequest((DataPLRequest) request,
								                  request.getSchedTimestamp() + rtoDataMap.get(request.getPort()).getRTO()));
						}
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
						sendCoordinatorCondition.awaitUntil(new Date(request.getSchedTimestamp()));
						
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
			PLMessage msg = request.toPLMessage(plRecvPort);
			byte[] plMessageBytes = msg.getBytes();
			
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
			
			// Process according to the type
			if (recvPLMessage.getMessageType() == PLMessage.PLMessageType.Ack)
			{
				waitingACKSet.remove(recvPLMessage.getSeqNum());
				
				synchronized (PerfectLink.class)
				{
					//// TCP's Retransmission Timer Algorithm [IETF RFC 6298] ////
					
					RTOData rtoData = rtoDataMap.get(recvPLMessage.getSenderRecvPort());
					long R = System.currentTimeMillis() - recvPLMessage.getSendTimestamp();
					int G = 1;
					int K = 4;
					double alpha = 1 / 8.;
					double beta = 1 / 4.;
					
					double newSRTT;
					double newRTTVAR;
					
					if (rtoData.isFirstRTT())
					{
						newSRTT = R;
						newRTTVAR = R / 2.;
					}
					else
					{
						newRTTVAR = (1 - beta) * rtoData.getRTTVAR() + beta * Math.abs(rtoData.getSRTT() - R);
						newSRTT = (1 - alpha) * rtoData.getSRTT() + alpha * R;
					}
					
					long newRTO = (long) (newSRTT + Math.max(G, K * newRTTVAR));
					
					////
					
					setRTOData(recvPLMessage.getSenderRecvPort(), false, newSRTT, newRTTVAR, newRTO);
				}
			}
			else // Normal
			{
				// Send ACK
				addRequestToSendQueue(new AckPLRequest(System.currentTimeMillis(),
				                                       recvPLMessage.getSeqNum(),
				                                       senderAddress,
				                                       recvPLMessage.getSenderRecvPort(),
				                                       recvPLMessage.getSendTimestamp()));
				
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
		synchronized (PerfectLink.class)
		{
			if (!rtoDataMap.containsKey(port))
				setRTOData(port, true, 0., 0., 500); // EDIT (original: 1000)
		}
		
		long seqNum = nextSeqNum.getAndIncrement();
		waitingACKSet.add(seqNum);
		addRequestToSendQueue(new DataPLRequest(System.currentTimeMillis(), seqNum, address, port, data));
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
