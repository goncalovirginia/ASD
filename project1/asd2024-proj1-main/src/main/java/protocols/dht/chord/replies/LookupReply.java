package protocols.dht.chord.replies;

import org.apache.commons.lang3.tuple.Pair;
import protocols.dht.chord.messages.FoundSuccessorMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.network.data.Host;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class LookupReply extends ProtoReply {

	public final static short REPLY_ID = 502;

	private final BigInteger key;
	private final UUID mid;
	private final List<Pair<BigInteger, Host>> peers;

	public LookupReply(FoundSuccessorMessage foundSuccessorMessage) {
		super(REPLY_ID);
		this.key = foundSuccessorMessage.getKey();
		this.mid = foundSuccessorMessage.getMid();
		this.peers = new LinkedList<>();
	}

	public BigInteger getKey() {
		return this.key;
	}

	public UUID getMid() {
		return this.mid;
	}

	public Iterator<Pair<BigInteger, Host>> getPeersIterator() {
		return this.peers.iterator();
	}

	public void addElementToPeers(BigInteger peerID, Host h) {
		this.peers.add(Pair.of(peerID, h));
	}

	public String toString() {
		StringBuilder reply = new StringBuilder("LookupReply for key: " + this.key + " containing set (" + this.peers.size() + " elements):\n");
		for (Pair<BigInteger, Host> p : this.peers) {
			reply.append("\t").append(p.getRight()).append("\n");
		}
		return reply.toString();
	}

}
