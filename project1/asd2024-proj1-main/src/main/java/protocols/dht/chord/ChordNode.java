package protocols.dht.chord;

import pt.unl.fct.di.novasys.network.data.Host;

import java.math.BigInteger;

public class ChordNode {

	private final BigInteger peerID;
	private final byte[] peerIDBytes;
	private final Host host;

	public ChordNode(BigInteger peerID, Host host) {
		this.peerID = peerID;
		this.peerIDBytes = peerID.toByteArray();
		this.host = host;
	}

	public BigInteger getPeerID() {
		return peerID;
	}

	public byte[] getPeerIDBytes() {
		return peerIDBytes;
	}

	public Host getHost() {
		return host;
	}

}
