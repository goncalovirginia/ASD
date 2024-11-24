package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ChangeMembershipMessage extends ProtoMessage {

    public final static short MSG_ID = 192;

    private final Host replica;
    private final int instance;
    private final boolean adding;
    private final boolean ok;

    public ChangeMembershipMessage(Host newReplica, int instance, boolean ok, boolean adding) {
        super(MSG_ID);
        this.replica = newReplica;
        this.instance = instance;
        this.ok = ok;
        this.adding = adding;
    }

    public Host getReplica() {
        return replica;
    }

    public int getInstance() {
        return instance;
    }

    public boolean isOK() {
        return ok;
    }

    public boolean isAdding() {
        return adding;
    }

    @Override
    public String toString() {
        return "ChangeMembershipMessage{" +
                "replica=" + replica +
                ", instance=" + instance +
                '}';
    }

    public static ISerializer<ChangeMembershipMessage> serializer = new ISerializer<ChangeMembershipMessage>() {
        @Override
        public void serialize(ChangeMembershipMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.replica, out);
            out.writeInt(msg.instance);
            out.writeBoolean(msg.ok);
            out.writeBoolean(msg.adding);
        }

        @Override
        public ChangeMembershipMessage deserialize(ByteBuf in) throws IOException {
            Host nReplica = Host.serializer.deserialize(in);
            int instance = in.readInt();
            boolean ok = in.readBoolean();
            boolean add = in.readBoolean();
            return new ChangeMembershipMessage(nReplica, instance, ok, add);
        }
    };

}
