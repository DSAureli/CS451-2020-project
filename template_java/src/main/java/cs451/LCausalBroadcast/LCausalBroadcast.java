package cs451.LCausalBroadcast;

import cs451.Host;
import cs451.UniformReliableBroadcast.UniformReliableBroadcast;

import java.io.NotSerializableException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class LCausalBroadcast
{
	// Waiting Causal Broadcast
	
	private final Lock lcLock = new ReentrantLock();
	private final BlockingQueue<LCMessage> pendingDeliverQueue = new LinkedBlockingQueue<>();
	private final Thread deliverThread;
	
	private final int id;
	private final Set<Integer> hostDependencySet; // <hosts on which self host depends>
	private final Map<Integer, Set<Integer>> hostInfluenceMap; // <host, hosts that depend on key host>
	
	private final HashMap<Integer, PriorityQueue<LCMessage>> pendingMsgQueueMap; // <HostId, Queue<Msg>>
	private final HashMap<Integer, Integer> lastDeliveredIdxMap; // <HostId, MsgIdx>
	private Integer localSequenceNumber;
	
	private static final int brWindowSize = 1000;
	private final Lock brWindowLock = new ReentrantLock();
	private final Condition brWindowCondition = brWindowLock.newCondition();
	private final AtomicInteger brWindowAvailableSlots = new AtomicInteger(brWindowSize); // broadcast window
	
	private final Consumer<Message> broadcastCallback;
	private final Consumer<List<Message>> deliverCallback;
	
	private final UniformReliableBroadcast urb;
	
	public LCausalBroadcast(int id,
	                        List<Host> hosts,
	                        Map<Integer, Set<Integer>> hostDependencyMap,
	                        Consumer<Message> broadcastCallback,
	                        Consumer<List<Message>> deliverCallback,
	                        int threadPoolSize) throws SocketException, UnknownHostException
	{
		this.id = id;
		this.hostDependencySet = hostDependencyMap.get(id);
		
		this.hostInfluenceMap = new HashMap<>(hosts.size());
		hosts.stream().map(Host::getId).forEach(hostId -> this.hostInfluenceMap.put(hostId, new HashSet<>(hosts.size())));
		for (Map.Entry<Integer, Set<Integer>> entry : hostDependencyMap.entrySet())
		{
			for (Integer influencingHostId : entry.getValue())
			{
				this.hostInfluenceMap.get(influencingHostId).add(entry.getKey());
			}
		}
		
		this.broadcastCallback = broadcastCallback;
		this.deliverCallback = deliverCallback;
		
		this.pendingMsgQueueMap = new HashMap<>(hosts.size());
		this.lastDeliveredIdxMap = new HashMap<>(hosts.size());
		this.localSequenceNumber = 0;
		
		for (Host host: hosts)
		{
			this.pendingMsgQueueMap.put(host.getId(),
			                            new PriorityQueue<>(1,
			                                                Comparator.comparingInt(lcMsg -> lcMsg.getMessage().getIdx())));
			this.lastDeliveredIdxMap.put(host.getId(), 0);
		}
		
		List<Host> targetHosts = new ArrayList<>(hosts);
		Host selfHost = targetHosts.stream().filter(host -> host.getId() == id).findFirst().get();
		targetHosts.remove(selfHost);
		
		this.urb = new UniformReliableBroadcast(selfHost, targetHosts, this::deliver, threadPoolSize);
		
		this.deliverThread = new Thread(new DeliverThread());
		this.deliverThread.start();
	}
	
	private class DeliverThread implements Runnable
	{
		@Override
		public void run()
		{
			while (!Thread.interrupted())
			{
				// Retrieve all pending deliveries, blocking if none available
				
				List<LCMessage> retrievedList = new LinkedList<>();
				try
				{
					retrievedList.add(pendingDeliverQueue.take());
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
					Thread.currentThread().interrupt();
				}
				pendingDeliverQueue.drainTo(retrievedList);
				
				lcLock.lock();
				
				// List of messages to deliver in batch
				List<Message> deliveringMsgList = new LinkedList<>();
				
				// Id's of hosts which pending queue has to be checked for possible deliveries, as they are dependant to
				// a host whose a message was recently delivered
				Queue<Integer> toCheckPendingQueueIdQueue = new LinkedList<>();
				
				// Add retrieved messages to the pending queue and add their sender host id to the queue of host id's to
				// be checked for possible deliveries
				
				for (LCMessage lcMessage : retrievedList)
				{
					pendingMsgQueueMap.get(lcMessage.getMessage().getHost()).add(lcMessage);
					toCheckPendingQueueIdQueue.add(lcMessage.getMessage().getHost());
				}
				
				while (!toCheckPendingQueueIdQueue.isEmpty())
				{
					int toCheckQueueId = toCheckPendingQueueIdQueue.poll();
					
					// Check if the message with the lowest index for the host id's to be checked can be delivered
					
					// No need to loop over the pending queues, just add the inverse relation to the queue (self host is included!)
					LCMessage toCheckLCMsg = pendingMsgQueueMap.get(toCheckQueueId).peek();
					boolean canDeliver = toCheckLCMsg != null &&
						toCheckLCMsg.getDependenciesMap().entrySet().stream()
							.allMatch(entry -> entry.getValue() <= lastDeliveredIdxMap.get(entry.getKey()));
					
					if (canDeliver)
					{
						// Add message to the delivery list and add the id's of hosts that depend on the sender of the
						// message to the queue of id's to be checked, as it is possible that this delivery unlocks them
						
						Message deliveringMsg = pendingMsgQueueMap.get(toCheckQueueId).poll().getMessage();
						deliveringMsgList.add(deliveringMsg);
						lastDeliveredIdxMap.put(deliveringMsg.getHost(), deliveringMsg.getIdx());
						toCheckPendingQueueIdQueue.addAll(hostInfluenceMap.get(deliveringMsg.getHost()));
					}
				}
				
				// Deliver
				
				if (!deliveringMsgList.isEmpty())
					deliverCallback.accept(deliveringMsgList);
				
				lcLock.unlock();
			}
		}
	}
	
	private void deliver(String msg)
	{
		// Add message to delivery queue
		
		LCMessage lcMessage;
		try
		{
			lcMessage = LCMessage.fromString(msg);
		}
		catch (NotSerializableException e)
		{
			e.printStackTrace();
			return;
		}
		
		pendingDeliverQueue.add(lcMessage);
		
		// Free broadcasting window slot if delivering message was broadcast by self
		
		if (lcMessage.getMessage().getHost() == id)
		{
			brWindowLock.lock();
			brWindowAvailableSlots.getAndIncrement();
			brWindowCondition.signal();
			brWindowLock.unlock();
		}
	}
	
	public void broadcast(Message msg) throws InterruptedException
	{
		// First block on broadcasting window
		
		brWindowLock.lock();
		if (brWindowAvailableSlots.get() < 1)
			brWindowCondition.await();
		brWindowAvailableSlots.getAndDecrement();
		brWindowLock.unlock();
		
		// Broadcast should block until broadcast message is written to file
		lcLock.lock();
		
		Map<Integer, Integer> dependenciesMap = lastDeliveredIdxMap.entrySet().stream()
			.filter(entry -> hostDependencySet.contains(entry.getKey()))
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		
		dependenciesMap.put(id, localSequenceNumber);
		localSequenceNumber = msg.getIdx();
		
		// This needs to lock out the deliveries, otherwise we may broadcast m2 that depends on m1, while having
		// d m1, d m3, b m2 in the logs because the delivery of m3 happened concurrently
		broadcastCallback.accept(msg);
		
		// Broadcast message written to file, release lock
		lcLock.unlock();
		
		// This was originally here after the unlock since everything could get stuck if urb.broadcast blocked (i.e. all
		// processes broadcast at the same time and block while holding the lock).
		// This is not the case anymore, but we keep it here for performance reasons: releasing the lock early allows
		// others broadcasts and deliveries to proceed and does not affect the semantics of LCB, since we already
		// computed the dependencies and wrote the broadcast message "atomically" (w.r.t this instance of LCB).
		LCMessage lcMessage = new LCMessage(msg, dependenciesMap);
		urb.broadcast(lcMessage.toString());
	}
	
	public void close()
	{
		urb.close();
		deliverThread.interrupt();
	}
}
