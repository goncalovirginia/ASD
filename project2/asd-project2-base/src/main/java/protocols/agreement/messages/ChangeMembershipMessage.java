package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class ChangeMembershipMessage extends ProtoMessage {

    public final static short MSG_ID = 192;

    private final Host replica;
    private final int instance;
    private final boolean adding;
    private final boolean ok;
    private final List<Pair<UUID, byte[]>> toBeDecidedMessages;

    public ChangeMembershipMessage(Host newReplica, int instance, boolean ok, boolean adding) {
        super(MSG_ID);
        this.replica = newReplica;
        this.instance = instance;
        this.ok = ok;
        this.adding = adding;
        this.toBeDecidedMessages = new LinkedList<>();
    }

    public ChangeMembershipMessage(Host newReplica, int instance, boolean ok, boolean adding, List<Pair<UUID, byte[]>> toBeDecidedMessages) {
        super(MSG_ID);
        this.replica = newReplica;
        this.instance = instance;
        this.ok = ok;
        this.adding = adding;
        this.toBeDecidedMessages = toBeDecidedMessages;
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

    public List<Pair<UUID, byte[]>> getToBeDecidedMessages() {
        return toBeDecidedMessages;
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

            List<Pair<UUID, byte[]>> messages = msg.getToBeDecidedMessages();
            out.writeInt(messages.size());
            for (Pair<UUID, byte[]> pair : messages) {
                UUID uuid = pair.getLeft();
                out.writeLong(uuid.getMostSignificantBits());
                out.writeLong(uuid.getLeastSignificantBits());

                byte[] data = pair.getRight();
                out.writeInt(data.length);
                out.writeBytes(data);
            }
        }

        @Override
        public ChangeMembershipMessage deserialize(ByteBuf in) throws IOException {
            Host nReplica = Host.serializer.deserialize(in);
            int instance = in.readInt();
            boolean ok = in.readBoolean();
            boolean add = in.readBoolean();

            int size = in.readInt();
            List<Pair<UUID, byte[]>> messages = new LinkedList<>();
            for (int i = 0; i < size; i++) {
                long mostSigBits = in.readLong();
                long leastSigBits = in.readLong();
                UUID uuid = new UUID(mostSigBits, leastSigBits);

                int dataLength = in.readInt();
                byte[] data = new byte[dataLength];
                in.readBytes(data);

                messages.add(Pair.of(uuid, data));
            }
            return new ChangeMembershipMessage(nReplica, instance, ok, add, messages);
        }
    };
}
