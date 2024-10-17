package protocols.dht.kademlia.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class TCPChannelCreatedNotification extends ProtoNotification {

	public static final short NOTIFICATION_ID = 501;

	private final int channelId;

	public TCPChannelCreatedNotification(int channelId) {
		super(NOTIFICATION_ID);
		this.channelId = channelId;
	}

	public int getChannelId() {
		return channelId;
	}
}
