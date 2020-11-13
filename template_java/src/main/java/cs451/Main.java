package cs451;

import cs451.Parser.Parser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class Main
{
	private static final int threadPoolSize = Math.min(1, (Runtime.getRuntime().availableProcessors() / 2) - 1);
	
	// BufferedWriter is thread-safe
	private static BufferedWriter fileWriter;
	
	private static FIFO fifo;
	
	private static void handleSignal() throws IOException
	{
		// immediately stop network packet processing
		System.out.println("Immediately stopping network packet processing.");
		fifo.close();
		
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
		
		int msgCount;
		if (parser.hasConfig())
		{
			BufferedReader fileReader = Files.newBufferedReader(Paths.get(parser.config()));
			msgCount = Integer.parseInt(fileReader.readLine());
		}
		else
		{
			msgCount = 100;
		}
		
		
		// TODO [DEBUG]
		int toDeliverCount = parser.hosts().size() * msgCount;
		AtomicInteger deliveredCount = new AtomicInteger();
		long startTime = System.currentTimeMillis();
		
		
		fileWriter = new BufferedWriter(new FileWriter(parser.output()));
		
		fifo = new FIFO(parser.hosts(),
		                parser.myId(),
		                (messageList) -> {
							messageList.forEach(msg -> {
				                try
				                {
					                fileWriter.append(String.format("d %s%n", msg));
					                System.out.printf("d %s%n", msg);
					
					                // TODO [DEBUG]
					                if (deliveredCount.incrementAndGet() == toDeliverCount)
						                System.out.printf("[END] Time: %d ms%n", System.currentTimeMillis() - startTime);
				                }
				                catch (IOException e)
				                {
					                e.printStackTrace();
				                }
							});
		                },
		                threadPoolSize);
		
		System.out.println("Waiting for all processes for finish initialization");
		coordinator.waitOnBarrier();
		
		System.out.println("Broadcasting messages...");
		
		for (int i = 1; i <= msgCount; i++)
		{
			try
			{
				fifo.broadcast(String.format("%d %d", parser.myId(), i));
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
