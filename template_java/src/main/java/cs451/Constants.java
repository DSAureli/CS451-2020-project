package cs451;

public class Constants
{
	public static final int ARG_LIMIT_NO_CONFIG = 10;
	public static final int ARG_LIMIT_CONFIG = 11;
	
	// indexes for id
	public static final int ID_KEY = 0;
	public static final int ID_VALUE = 1;
	
	// indexes for hosts
	public static final int HOSTS_KEY = 2;
	public static final int HOSTS_VALUE = 3;
	
	// indexes for barrier
	public static final int BARRIER_KEY = 4;
	public static final int BARRIER_VALUE = 5;
	
	// indexes for signal
	public static final int SIGNAL_KEY = 6;
	public static final int SIGNAL_VALUE = 7;
	
	// indexes for output
	public static final int OUTPUT_KEY = 8;
	public static final int OUTPUT_VALUE = 9;
	
	// indexes for config
	public static final int CONFIG_VALUE = 10;
	
	////
	
	// ASCII Control Codes
	public static class CC
	{
		public static char SOH = (char) 1; // start of heading
		public static char STX = (char) 2; // start of text
		public static char EOT = (char) 4; // end of transmission
		public static char ENQ = (char) 5; // enquiry
		public static char ACK = (char) 6; // acknowledge
		public static char FS = (char) 28; // file separator
		public static char RS = (char) 30; // record separator
	}
}
