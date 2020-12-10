package cs451;

import cs451.LCausalBroadcast.LCausalBroadcast;
import cs451.LCausalBroadcast.Message;
import cs451.Parser.Parser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Main
{
	private static final int threadPoolSize = Math.max(2, Runtime.getRuntime().availableProcessors() - 4) / 2;
	
	// BufferedWriter is thread-safe
	private static BufferedWriter fileWriter;
	
	private static LCausalBroadcast lCausalBroadcast;
	
	private static void handleSignal() throws IOException
	{
		// immediately stop network packet processing
		System.out.println("Immediately stopping network packet processing.");
		if (lCausalBroadcast != null)
			lCausalBroadcast.close();
		
		// write/flush output file if necessary
		if (fileWriter != null)
		{
			System.out.println("Writing output.");
			fileWriter.close();
		}
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
		else
		{
			System.out.println("Config is missing! Aborting...");
			System.exit(1);
		}
		
		////
		
		Coordinator coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());
		
		int msgCount;
		Map<Integer, Set<Integer>> hostDependencyMap = new HashMap<>(parser.hosts().size());

		BufferedReader fileReader = Files.newBufferedReader(Paths.get(parser.config()));
		msgCount = Integer.parseInt(fileReader.readLine());
		
		for (int lineNumber = 1; lineNumber <= parser.hosts().size(); lineNumber++)
		{
			try
			{
				hostDependencyMap.put(lineNumber,
				                      Arrays.stream(fileReader.readLine().split(" "))
					                      .map(Integer::parseInt)
					                      .collect(Collectors.toSet()));
			}
			catch (NullPointerException e)
			{
				e.printStackTrace();
				System.out.println("Config is malformed! Aborting...");
				System.exit(1);
			}
		}
		
		////
		
		fileWriter = new BufferedWriter(new FileWriter(parser.output()));
		
		Consumer<Message> broadcastCallback = msg -> {
			try
			{
				String out = String.format("b %s%n", msg.getIdx());
				fileWriter.append(out);
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		};
		
		Consumer<List<Message>> deliverCallback = msgList -> {
			for (Message msg: msgList)
			{
				try
				{
					String out = String.format("d %s%n", msg);
					fileWriter.append(out);
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		};
		
		lCausalBroadcast = new LCausalBroadcast(parser.myId(), parser.hosts(), hostDependencyMap,
		                                        broadcastCallback, deliverCallback, threadPoolSize);
		
		System.out.println("Waiting for all processes for finish initialization");
		coordinator.waitOnBarrier();
		
		System.out.println("Broadcasting messages...");
		
		for (int it = 1; it <= msgCount; it++)
		{
			lCausalBroadcast.broadcast(new Message(parser.myId(), it));
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
