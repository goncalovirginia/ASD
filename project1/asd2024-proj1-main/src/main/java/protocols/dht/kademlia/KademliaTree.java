package protocols.dht.kademlia;

import java.math.BigInteger;

public class KademliaTree {

	private final KademliaNode thisNode;

	public KademliaTree(KademliaNode thisNode) {
		this.thisNode = thisNode;
	}

	public KademliaNode getThisNode() {
		return thisNode;
	}

	private BigInteger xor(BigInteger a, BigInteger b) {
		return a.xor(b);
	}

}
