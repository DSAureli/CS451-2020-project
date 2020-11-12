package cs451;

import cs451.Parser.Parser;
import cs451.UniformReliableBroadcast.UniformReliableBroadcast;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class Main
{
	private static final ExecutorService recvThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	// all ExecutorService implementations should be thread safe for task submission
	
	// BufferedWriter is thread-safe
	private static BufferedWriter fileWriter;
	
	private static UniformReliableBroadcast uniformReliableBroadcast;
	
	private static void handleSignal() throws IOException
	{
		// immediately stop network packet processing
		System.out.println("Immediately stopping network packet processing.");
		uniformReliableBroadcast.close();
		
		// write/flush output file if necessary
		System.out.println("Writing output.");
		fileWriter.close();
	}
	
	private static void initSignalHandlers()
	{
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try
			{
				handleSignal();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}));
	}
	
	public static void main(String[] args) throws InterruptedException, IOException
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
		fileWriter = new BufferedWriter(new FileWriter(parser.output()));
		
		List<Host> targetHosts = parser.hosts();
		Host self = targetHosts.stream().filter(host -> host.getId() == parser.myId()).findFirst().get();
		targetHosts.remove(self);
		
		uniformReliableBroadcast = new UniformReliableBroadcast(self,
		                                                        targetHosts,
		                                                        (message) -> {
																	try
																	{
																		fileWriter.append(String.format("d %s%n", message));
																		System.out.printf("d %s%n", message);
																	}
																	catch (IOException e)
																	{
																		e.printStackTrace();
																	}
																},
		                                                        recvThreadPool);
		
		System.out.println("Waiting for all processes for finish initialization");
		coordinator.waitOnBarrier();
		
		System.out.println("Broadcasting messages...");
		
		for (int i = 0; i < 5; i++)
		{
			try
			{
				uniformReliableBroadcast.broadcast(String.format("%d %d", self.getId(), i));
				fileWriter.append(String.format("b %s%n", i));
				System.out.printf("b %s%n", i);
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
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
