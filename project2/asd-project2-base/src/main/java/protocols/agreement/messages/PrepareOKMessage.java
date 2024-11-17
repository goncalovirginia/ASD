package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

//ADD MESSAGES OF INSTANCES > CURR LEADER OPSEQ
public class PrepareOKMessage extends ProtoMessage {

    public final static short MSG_ID = 107;

    private final int instance;

    public PrepareOKMessage(int instance) {
        super(MSG_ID);
        this.instance = instance;
    }

    public int getInstance() {
        return instance;
    }

    @Override
    public String toString() {
        return "PrepareOKMessage{" +
                "instance=" + instance +
                '}';
    }

    public static ISerializer<PrepareOKMessage> serializer = new ISerializer<PrepareOKMessage>() {
        @Override
        public void serialize(PrepareOKMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
        }

        @Override
        public PrepareOKMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            return new PrepareOKMessage(instance);
        }
    };

}
