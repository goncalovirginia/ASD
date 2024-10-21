package protocols.point2point.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class DHTInitializedNotification extends ProtoNotification {

	public static final short NOTIFICATION_ID = 402;

	public DHTInitializedNotification() {
		super(NOTIFICATION_ID);
	}

}
