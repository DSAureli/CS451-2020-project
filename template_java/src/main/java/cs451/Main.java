package cs451;

import cs451.Parser.Parser;

import java.net.InetAddress;

public class Main
{
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
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
			@Override
			public void run()
			{
				handleSignal();
			}
		});
	}
	
	public static void main(String[] args) throws InterruptedException
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
		
		Host host = parser.hosts().get(parser.myId() - 1);
		
		try
		{
			PerfectLink perfectLink = new PerfectLink(host.getPort());
			perfectLink.startReceiving();

			for (int i = 0; i < 3; i++)
			{
				perfectLink.send(String.format("test_%d", i), InetAddress.getByName(host.getIp()), host.getPort());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
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
