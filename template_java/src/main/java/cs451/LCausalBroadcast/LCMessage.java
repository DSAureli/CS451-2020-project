package cs451.LCausalBroadcast;

import java.io.NotSerializableException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LCMessage
{
	private final Message message;
	private final Map<Integer, Integer> dependenciesMap;
	
	public Message getMessage()
	{
		return message;
	}
	
	public Map<Integer, Integer> getDependenciesMap()
	{
		return dependenciesMap;
	}
	
	public LCMessage(Message message, Map<Integer, Integer> dependenciesMap)
	{
		this.message = message;
		this.dependenciesMap = dependenciesMap;
	}
	
	private static void error(String error) throws NotSerializableException
	{
		throw new NotSerializableException("[LCMessage] " + error);
	}
	
	public static LCMessage fromString(String str) throws NotSerializableException
	{
		String[] parts = str.split(Pattern.quote("|"));
		if (parts.length != 2)
			error("missing |");
		
		String[] newMessageParts = parts[0].split(" ");
		if (newMessageParts.length != 2)
			error("malformed message");
		Message newMessage = new Message(Integer.parseInt(newMessageParts[0]), Integer.parseInt(newMessageParts[1]));
		
		Map<Integer, Integer> newDependenciesMap = new HashMap<>();
		
		if (!parts[1].isEmpty())
		{
			String[] depParts = parts[1].split(",");
			
			for (String depStr : depParts)
			{
				String[] depStrTokens = depStr.split(":");
				if (depStrTokens.length != 2)
					error("malformed dependencies");
				
				newDependenciesMap.put(Integer.valueOf(depStrTokens[0]), Integer.valueOf(depStrTokens[1]));
			}
		}
		
		return new LCMessage(newMessage, newDependenciesMap);
	}
	
	@Override
	public String toString()
	{
		String depString = dependenciesMap.entrySet().stream()
			.map(entry -> String.format("%d:%d", entry.getKey(), entry.getValue()))
			.collect(Collectors.joining(","));
		return String.format("%s|%s", message, depString);
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		LCMessage lcMessage = (LCMessage) o;
		return Objects.equals(message, lcMessage.message) &&
			Objects.equals(dependenciesMap, lcMessage.dependenciesMap);
	}
	
	@Override
	public int hashCode()
	{
		return Objects.hash(message, dependenciesMap);
	}
}
