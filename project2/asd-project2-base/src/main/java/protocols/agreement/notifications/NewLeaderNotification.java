package protocols.agreement.notifications;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class NewLeaderNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 403;

    private final Host leader;
    private List<Pair<UUID, byte[]>> prepareOKMessages;

    public NewLeaderNotification(Host membership) {
        super(NOTIFICATION_ID);
        this.leader = membership;
        prepareOKMessages = new LinkedList<>();
    }

    public NewLeaderNotification(Host membership, List<Pair<UUID, byte[]>> prepareOKMessages) {
        super(NOTIFICATION_ID);
        this.leader = membership;
        this.prepareOKMessages = prepareOKMessages;
    }

    public Host getLeader() {
        return leader;
    }

    public List<Pair<UUID, byte[]>> getMessages() {
        return prepareOKMessages;
    }

    @Override
    public String toString() {
        return "NewLeaderNotification{" +
                "leader=" + leader + '}';
    }
}
