package protocols.dht.chord.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class DHTInitializedNotification extends ProtoNotification {

	public static final short NOTIFICATION_ID = 402;

	private final boolean isInitialized;

	public DHTInitializedNotification(boolean isInitialized) {
		super(NOTIFICATION_ID);
		this.isInitialized = isInitialized;
	}

	public boolean isInitialized() {
		return isInitialized;
	}

}
