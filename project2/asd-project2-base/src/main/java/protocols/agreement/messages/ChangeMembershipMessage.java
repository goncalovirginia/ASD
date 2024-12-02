package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class ChangeMembershipMessage extends ProtoMessage {

    public final static short MSG_ID = 192;

    private final Host replica;
    private final int instance;
    private final int sequenceNumber;
    private final boolean adding;
    private final boolean ok;
    private final Map<Integer, Pair<UUID, byte[]>> toBeDecidedMessages;
    private final Map<Integer, Pair<Host, Boolean>> addReplicaInstances;

    public ChangeMembershipMessage(Host newReplica, int instance, int sn, boolean ok, boolean adding) {
        super(MSG_ID);
        this.replica = newReplica;
        this.instance = instance;
        this.sequenceNumber = sn;
        this.ok = ok;
        this.adding = adding;
        this.toBeDecidedMessages = new TreeMap<>();
        this.addReplicaInstances = new TreeMap<>();
    }

    public ChangeMembershipMessage(Host newReplica, int instance, int sn, boolean ok, boolean adding,
                                   Map<Integer, Pair<UUID, byte[]>> toBeDecidedMessages,
                                   Map<Integer, Pair<Host, Boolean>> addReplicaInstances) {
        super(MSG_ID);
        this.replica = newReplica;
        this.instance = instance;
        this.sequenceNumber = sn;
        this.ok = ok;
        this.adding = adding;
        this.toBeDecidedMessages = new TreeMap<>(toBeDecidedMessages);
        this.addReplicaInstances = new TreeMap<>(addReplicaInstances);
    }

    public Host getReplica() {
        return replica;
    }

    public int getInstance() {
        return instance;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public boolean isOK() {
        return ok;
    }

    public boolean isAdding() {
        return adding;
    }

    public Map<Integer, Pair<UUID, byte[]>> getToBeDecidedMessages() {
        return toBeDecidedMessages;
    }

    public Map<Integer, Pair<Host, Boolean>> getAddReplicaInstances() {
        return addReplicaInstances;
    }

    public int AddReplicaInstancesSize() {
        return addReplicaInstances.size();
    }

    @Override
    public String toString() {
        return "ChangeMembershipMessage{" +
                "replica=" + replica +
                ", instance=" + instance +
                ", adding=" + adding +
                ", ok=" + ok +
                '}';
    }

    public static ISerializer<ChangeMembershipMessage> serializer = new ISerializer<ChangeMembershipMessage>() {
        @Override
        public void serialize(ChangeMembershipMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.replica, out);
            out.writeInt(msg.instance);
            out.writeInt(msg.sequenceNumber);
            out.writeBoolean(msg.ok);
            out.writeBoolean(msg.adding);

            Map<Integer, Pair<UUID, byte[]>> messages = msg.getToBeDecidedMessages();
            out.writeInt(messages.size());
            for (Map.Entry<Integer, Pair<UUID, byte[]>> entry : messages.entrySet()) {
                out.writeInt(entry.getKey());
                UUID uuid = entry.getValue().getLeft();
                out.writeLong(uuid.getMostSignificantBits());
                out.writeLong(uuid.getLeastSignificantBits());

                byte[] data = entry.getValue().getRight();
                out.writeInt(data.length);
                out.writeBytes(data);
            }

            Map<Integer, Pair<Host, Boolean>> replicas = msg.getAddReplicaInstances();
            out.writeInt(replicas.size());
            for (Map.Entry<Integer, Pair<Host, Boolean>> entry : replicas.entrySet()) {
                out.writeInt(entry.getKey());
                Host.serializer.serialize(entry.getValue().getLeft(), out);
                out.writeBoolean(entry.getValue().getRight());
            }
        }

        @Override
        public ChangeMembershipMessage deserialize(ByteBuf in) throws IOException {
            Host nReplica = Host.serializer.deserialize(in);
            int instance = in.readInt();
            int sn = in.readInt();
            boolean ok = in.readBoolean();
            boolean add = in.readBoolean();

            int messageSize = in.readInt();
            TreeMap<Integer, Pair<UUID, byte[]>> messages = new TreeMap<>();
            for (int i = 0; i < messageSize; i++) {
                int key = in.readInt();
                long mostSigBits = in.readLong();
                long leastSigBits = in.readLong();
                UUID uuid = new UUID(mostSigBits, leastSigBits);

                int dataLength = in.readInt();
                byte[] data = new byte[dataLength];
                in.readBytes(data);

                messages.put(key, Pair.of(uuid, data));
            }

            int replicaSize = in.readInt();
            TreeMap<Integer, Pair<Host, Boolean>> replicas = new TreeMap<>();
            for (int i = 0; i < replicaSize; i++) {
                int key = in.readInt();
                Host host = Host.serializer.deserialize(in);
                boolean flag = in.readBoolean();
                replicas.put(key, Pair.of(host, flag));
            }

            return new ChangeMembershipMessage(nReplica, instance, sn, ok, add, new HashMap<>(messages), new HashMap<>(replicas));
        }
    };
}
