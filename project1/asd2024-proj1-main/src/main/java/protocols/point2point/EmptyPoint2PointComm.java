package protocols.point2point;

import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.dht.chord.notifications.TCPChannelCreatedNotification;
import protocols.dht.chord.replies.LookupReply;
import protocols.dht.chord.requests.LookupRequest;
import protocols.point2point.messages.Point2PointMessage;
import protocols.point2point.requests.Send;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class EmptyPoint2PointComm extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(EmptyPoint2PointComm.class);
	
	public final static String PROTOCOL_NAME = "EmptyPoint2PotinComm";
	public final static short PROTOCOL_ID = 400;
	
	private final Host thisHost;
	private final short DHT_PROTO_ID;
	
	public EmptyPoint2PointComm(Host thisHost, short DHT_Proto_ID) throws HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.thisHost = thisHost;
		this.DHT_PROTO_ID = DHT_Proto_ID;

		//register request handlers
		registerRequestHandler(Send.REQUEST_ID, this::uponSendRequest);

		//register reply handlers
		registerReplyHandler(LookupReply.REPLY_ID, this::uponLookupReply);

		//register notification handlers
		subscribeNotification(TCPChannelCreatedNotification.NOTIFICATION_ID, this::uponChannelCreated);
	}
	
	@Override
	public void init(Properties props) {

	}

	/*--------------------------------- Requests ---------------------------------------- */
	
	private void uponSendRequest(Send request, short protoID) {
		logger.info("Received Send Request: " + request.toString());
		
		LookupRequest lookupRequest = new LookupRequest(request.getDestinationPeerID());
		
		sendRequest(lookupRequest, DHT_PROTO_ID);
	}

	private void uponLookupReply(LookupReply reply, short protoID) { 
		logger.info("Received Lookup Reply: " + reply.toString());
		
	}

	/*--------------------------------- Messages ---------------------------------------- */

	private void uponPoint2PointMessage(Point2PointMessage point2PointMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received Point2Point Message: " + point2PointMessage.toString());
	}

	private void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*--------------------------------- Notifications ---------------------------------------- */

	//Upon receiving the channelId from the DHT algorithm, register our own callbacks and serializers
	private void uponChannelCreated(TCPChannelCreatedNotification notification, short sourceProto) {
		int channelId = notification.getChannelId();
		registerSharedChannel(channelId);

		//register message serializers
		registerMessageSerializer(channelId, Point2PointMessage.MSG_ID, Point2PointMessage.serializer);

		//register message handlers
		try {
			registerMessageHandler(channelId, Point2PointMessage.MSG_ID, this::uponPoint2PointMessage, this::uponMessageFail);
		} catch (HandlerRegistrationException e) {
			logger.error("Error registering message handler: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}
