package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import protocols.abd.utils.Tag;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareMessage extends ProtoMessage {

    public final static short MSG_ID = 106;

    private final Tag sequenceNumber;
    private final int instance;
    private final boolean ok;

    public PrepareMessage(Tag highest_prepare, int instance, boolean ok) {
        super(MSG_ID);
        this.sequenceNumber = highest_prepare;
        this.instance = instance;
        this.ok = ok;
    }

    public Tag getSeqNumber() {
        return sequenceNumber;
    }

    public int getInstance() {
        return instance;
    }

    public boolean isOK() {
        return ok;
    }

    @Override
    public String toString() {
        return "PrepareMessage{" +
                "seqNumber=" + sequenceNumber +
                '}';
    }

    public static ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            out.writeInt(msg.sequenceNumber.getOpSeq());
            out.writeInt(msg.sequenceNumber.getProcessId());
            out.writeInt(msg.instance);
            out.writeBoolean(msg.ok);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            int opSeq = in.readInt();
            int processId = in.readInt();
            Tag sequenceNumber = new Tag(opSeq, processId);
            int inst = in.readInt();
            boolean ok = in.readBoolean();
            return new PrepareMessage(sequenceNumber, inst, ok);
        }
    };

}
