package cs451;

import cs451.Parser.Parser;
import cs451.UniformReliableBroadcast.UniformReliableBroadcast;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main
{
	private static final ExecutorService recvThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	// all ExecutorService implementations should be thread safe for task submission
	
	private static void handleSignal()
	{
		//immediately stop network packet processing
		System.out.println("Immediately stopping network packet processing.");
		// TODO ...
		
		//write/flush output file if necessary
		System.out.println("Writing output.");
		// TODO ...
	}
	
	private static void initSignalHandlers()
	{
		Runtime.getRuntime().addShutdownHook(new Thread(Main::handleSignal));
	}
	
	public static void main(String[] args) throws InterruptedException, SocketException, UnknownHostException
	{
		Parser parser = new Parser(args);
		parser.parse();
		
		initSignalHandlers();
		
		// example
		long pid = ProcessHandle.current().pid();
		System.out.println("My PID is " + pid + ".");
		System.out.println("Use 'kill -SIGINT " + pid + " ' or 'kill -SIGTERM " + pid + " ' to stop processing packets.");
		
		System.out.println("My id is " + parser.myId() + ".");
		System.out.println("List of hosts is:");
		for (Host host : parser.hosts())
		{
			System.out.println(host.getId() + ", " + host.getIp() + ", " + host.getPort());
		}
		
		System.out.println("Barrier: " + parser.barrierIp() + ":" + parser.barrierPort());
		System.out.println("Signal: " + parser.signalIp() + ":" + parser.signalPort());
		System.out.println("Output: " + parser.output());
		// if config is defined; always check before parser.config()
		if (parser.hasConfig())
		{
			System.out.println("Config: " + parser.config());
		}
		
		
		Coordinator coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());
		
		System.out.println("Waiting for all processes for finish initialization");
		coordinator.waitOnBarrier();
		
		System.out.println("Broadcasting messages...");
		
		//// TODO
		
		List<Host> targetHosts = parser.hosts();
//		Host self = targetHosts.remove(parser.myId() - 1);
		Host self = targetHosts.stream().filter(host -> host.getId() == parser.myId()).findFirst().get();
		targetHosts.remove(self);
		
//		System.out.printf("targetHosts: %s%n", targetHosts);
//		System.out.printf("self: %s%n", self);
		
		UniformReliableBroadcast uniformReliableBroadcast
			= new UniformReliableBroadcast(self,
			                               targetHosts,
			                               (message) -> System.out.printf("[Delivered] %s%n", message),
			                               recvThreadPool);
		
		for (int i = 0; i < 5; i++)
		{
			uniformReliableBroadcast.broadcast(String.format("%d %d", self.getId(), i));
		}
		
		////
		
		System.out.println("Signaling end of broadcasting messages");
		coordinator.finishedBroadcasting();
		
		while (true)
		{
			// Sleep for 1 hour
			Thread.sleep(60 * 60 * 1000);
		}
	}
}
