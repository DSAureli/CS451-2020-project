package cs451.LCausalBroadcast;

import cs451.Host;
import cs451.UniformReliableBroadcast.UniformReliableBroadcast;

import java.io.NotSerializableException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class LCausalBroadcast
{
	Lock lock = new ReentrantLock();
	
	private final int id;
	private final Set<Integer> hostDependencySet;
	private final Map<Integer, Set<Integer>> hostInfluenceMap;
	
	private final HashMap<Integer, PriorityQueue<LCMessage>> pendingMsgQueueMap; // <HostId, Queue<Msg>>
	private final HashMap<Integer, Integer> lastDeliveredIdxMap; // <HostId, MsgIdx>
	private Integer localSequenceNumber;
	
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
		
		this.hostInfluenceMap = new HashMap<>();
		hosts.stream().map(Host::getId).forEach(hostId -> this.hostInfluenceMap.put(hostId, new HashSet<>()));
		for (Map.Entry<Integer, Set<Integer>> entry : hostDependencyMap.entrySet())
		{
			for (Integer influencingHostId : entry.getValue())
			{
				this.hostInfluenceMap.get(influencingHostId).add(entry.getKey());
			}
		}
		
		this.broadcastCallback = broadcastCallback;
		this.deliverCallback = deliverCallback;
		
		this.pendingMsgQueueMap = new HashMap<>();
		this.lastDeliveredIdxMap = new HashMap<>();
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
	}
	
	private void deliver(String msg)
	{
		// TODO de-synchronize
		lock.lock();
		
		LCMessage lcMessage;
		try
		{
			lcMessage = LCMessage.fromString(msg);
		}
		catch (NotSerializableException e)
		{
			e.printStackTrace();
			lock.unlock();
			return;
		}
		
		System.out.printf("=== lcMessage: %s%n", lcMessage);
		pendingMsgQueueMap.get(lcMessage.getMessage().getHost()).add(lcMessage);
		
		// List of messages to deliver in batch
		List<Message> deliveringMsgList = new LinkedList<>();
		
		// Id's of hosts which pending queue has to be checked for possible deliveries, as they are dependant to a
		// host whose a message was recently delivered
		Queue<Integer> toCheckPendingQueueIdQueue = new LinkedList<>();
		toCheckPendingQueueIdQueue.add(lcMessage.getMessage().getHost());
		
		while (!toCheckPendingQueueIdQueue.isEmpty())
		{
			int toCheckQueueId = toCheckPendingQueueIdQueue.poll();
			System.out.printf("=== toCheckQueueId: %s%n", toCheckQueueId);
			
			// TODO no need to loop over the pending queues, just add the inverse relation to the queue (self host is included!)
			LCMessage toCheckLCMsg = pendingMsgQueueMap.get(toCheckQueueId).peek();
			boolean canDeliver = toCheckLCMsg != null &&
				toCheckLCMsg.getDependenciesMap().entrySet().stream()
					.allMatch(entry -> entry.getValue() <= lastDeliveredIdxMap.get(entry.getKey()));
			
			System.out.printf("=== toCheckLCMsg: %s%n", toCheckLCMsg);
			System.out.printf("=== canDeliver: %s%n", canDeliver);
			
			if (canDeliver)
			{
				Message deliveringMsg = pendingMsgQueueMap.get(toCheckQueueId).poll().getMessage();
				deliveringMsgList.add(deliveringMsg);
				lastDeliveredIdxMap.put(deliveringMsg.getHost(), deliveringMsg.getIdx());
				toCheckPendingQueueIdQueue.addAll(hostInfluenceMap.get(deliveringMsg.getHost()));
			}
		}
		
		if (!deliveringMsgList.isEmpty())
			deliverCallback.accept(deliveringMsgList);
		
		// TODO de-synchronize
		lock.unlock();
	}
	
	// TODO broadcast should block until broadcast message is written to file
	// TODO check Main for this as well
	// TODO maybe I should pass a lambda here and call it while holding a lock
	public void broadcast(Message msg)
	{
		// TODO de-synchronize
		lock.lock();
		
		Map<Integer, Integer> dependenciesMap = lastDeliveredIdxMap.entrySet().stream()
			.filter(entry -> hostDependencySet.contains(entry.getKey()))
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		
		dependenciesMap.put(id, localSequenceNumber);
		localSequenceNumber += 1;
		
		LCMessage lcMessage = new LCMessage(msg, dependenciesMap);
		urb.broadcast(lcMessage.toString());
		
		System.out.printf("Broadcast: %s%n", lcMessage);
		
		// TODO this needs to lock out the deliveries, otherwise we may broadcast m2 that depends on m1, while having
		// TODO d m1, d m3, b m2 in the logs because the delivery of m3 happened concurrently
		broadcastCallback.accept(msg);
		
		// TODO de-synchronize
		lock.unlock();
	}
	
	public void close()
	{
		urb.close();
	}
}
