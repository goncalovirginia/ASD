package protocols.dht.chord.replies;

import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashProducer;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

public class LookupReply extends ProtoReply {

	public final static short REPLY_ID = 502;

	private final byte[] peerID;
	private final UUID mid;
	private final Set<Pair<byte[], Host>> peers;

	public LookupReply(byte[] peerID, UUID mid) {
		super(REPLY_ID);
		this.peerID = peerID.clone();
		this.mid = mid;
		this.peers = new HashSet<>();
	}

	public byte[] getPeerID() {
		return this.peerID.clone();
	}

	public BigInteger getPeerIDNumerical() {
		return HashProducer.toNumberFormat(peerID);
	}

	public String getPeerIDHex() {
		return HashProducer.toNumberFormat(peerID).toString(16);
	}

	public UUID getMid() {
		return this.mid;
	}

	public Iterator<Pair<byte[], Host>> getPeersIterator() {
		return this.peers.iterator();
	}

	public void addElementToPeers(byte[] peerID, Host h) {
		this.peers.add(Pair.of(peerID, h));
	}

	public String toString() {
		StringBuilder reply = new StringBuilder("LookupReply for " + this.getPeerIDHex() + " containing set (" + this.peers.size() + " elements):\n");
		for (Pair<byte[], Host> p : this.peers) {
			reply.append("\t").append(HashProducer.toNumberFormat(p.getLeft()).toString(16)).append("::").append(p.getRight().toString()).append("\n");
		}
		return reply.toString();
	}

}
