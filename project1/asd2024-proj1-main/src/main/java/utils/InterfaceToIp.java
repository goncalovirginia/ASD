package utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.InvalidParameterException;
import java.util.Enumeration;
import java.util.Properties;

public class InterfaceToIp {
	public static String getIpOfInterface(String interfaceName) throws SocketException {
		NetworkInterface networkInterface = NetworkInterface.getByName(interfaceName);
		System.out.println(networkInterface);
		Enumeration<InetAddress> inetAddress = networkInterface.getInetAddresses();
		InetAddress currentAddress;
		while (inetAddress.hasMoreElements()) {
			currentAddress = inetAddress.nextElement();
			if (currentAddress instanceof Inet4Address && !currentAddress.isLoopbackAddress()) {
				return currentAddress.getHostAddress();
			}
		}
		return null;
	}

	public static void addInterfaceIp(Properties props) throws SocketException, InvalidParameterException {
		String interfaceName;
		if ((interfaceName = props.getProperty("interface")) != null) {
			String ip = InterfaceToIp.getIpOfInterface(interfaceName);
			if (ip != null)
				props.setProperty("address", ip);
			else {
				throw new InvalidParameterException("Property interface is set to " + interfaceName + ", but has no ip");
			}
		}
	}
}
