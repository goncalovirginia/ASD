package protocols.dht.chord.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class PeerDownNotification extends ProtoNotification {

	public static final short NOTIFICATION_ID = 502;

	private final Host peer;

	public PeerDownNotification(Host peer) {
		super(NOTIFICATION_ID);
		this.peer = peer;
	}

	public Host getPeer() {
		return peer;
	}

}
