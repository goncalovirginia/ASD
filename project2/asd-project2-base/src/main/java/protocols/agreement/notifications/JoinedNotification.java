package protocols.agreement.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.List;

public class JoinedNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 402;

    private final List<Host> membership;
    private final int joinInstance;
    private final Host leader;

    public JoinedNotification(List<Host> membership, int joinInstance, Host leader) {
        super(NOTIFICATION_ID);
        this.membership = membership;
        this.joinInstance = joinInstance;
        this.leader = leader;
    }

    public int getJoinInstance() {
        return joinInstance;
    }

    public List<Host> getMembership() {
        return membership;
    }

    public Host getContact() {
        return leader;
    }

    @Override
    public String toString() {
        return "JoinedNotification{" +
                "membership=" + membership +
                ", joinInstance=" + joinInstance +
                '}';
    }
}
