package protocols.agreement.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class NewLeaderNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 403;

    private final Host leader;

    public NewLeaderNotification(Host membership) {
        super(NOTIFICATION_ID);
        this.leader = membership;
    }

    public Host getLeader() {
        return leader;
    }

    @Override
    public String toString() {
        return "NewLeaderNotification{" +
                "leader=" + leader + '}';
    }
}
