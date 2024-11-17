package protocols.agreement.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class MembershipChangedNotification extends ProtoNotification {

	public static final short NOTIFICATION_ID = 405;

	private final boolean adding;
	private final Host replica;

	public MembershipChangedNotification(Host replica, boolean adding) {
		super(NOTIFICATION_ID);
		this.adding = adding;
		this.replica = replica;
	}


	public Host getReplica() {
		return replica;
	}

	public boolean isAdding() {
		return adding;
	}

	@Override
	public String toString() {
		return "MembershipChangedNotification{" +
				"adding=" + adding +
				", replica=" + replica +
				'}';
	}
}
