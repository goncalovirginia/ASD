package protocols.point2point;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.chord.notifications.TCPChannelCreatedNotification;
import protocols.dht.chord.replies.LookupReply;
import protocols.dht.chord.requests.LookupRequest;
import protocols.point2point.messages.Point2PointMessage;
import protocols.point2point.notifications.DHTInitializedNotification;
import protocols.point2point.notifications.Deliver;
import protocols.point2point.requests.Send;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class Point2PointCommunicator extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(Point2PointCommunicator.class);

	public final static String PROTOCOL_NAME = "EmptyPoint2PointComm";
	public final static short PROTOCOL_ID = 400;

	private final Host thisHost;
	private final short DHT_PROTO_ID;

	private final List<Send> messagesPendingLookup;
	private final Map<UUID, Send> messagesPendingLookupReply;
	private final Set<UUID> receivedMessages;

    private boolean isDHTInitialized;

	public Point2PointCommunicator(Host thisHost, short DHT_Proto_ID) throws HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.thisHost = thisHost;
		this.DHT_PROTO_ID = DHT_Proto_ID;
		this.messagesPendingLookup = new LinkedList<>();
		this.messagesPendingLookupReply = new HashMap<>();
		this.receivedMessages = new HashSet<>();
		this.isDHTInitialized = false;

		//register request handlers
		registerRequestHandler(Send.REQUEST_ID, this::uponSendRequest);

		//register reply handlers
		registerReplyHandler(LookupReply.REPLY_ID, this::uponLookupReply);

		//register notification handlers
		subscribeNotification(TCPChannelCreatedNotification.NOTIFICATION_ID, this::uponChannelCreated);
		subscribeNotification(DHTInitializedNotification.NOTIFICATION_ID, this::uponDHTInitialized);
	}

	@Override
	public void init(Properties props) {

	}

	/*--------------------------------- Requests ---------------------------------------- */

	private void uponSendRequest(Send request, short protoID) {
		logger.info("Received SendRequest: " + request.getMessageID());

        if (!isDHTInitialized) {
            logger.info("DHT protocol is not initialized yet, storing message: " + request.getMessageID());
            messagesPendingLookup.add(request);
			return;
        }

		LookupRequest lookupRequest = new LookupRequest(request.getDestinationPeerID(), request.getMessageID());
		sendRequest(lookupRequest, DHT_PROTO_ID);
		messagesPendingLookupReply.put(request.getMessageID(), request);
	}

	/*--------------------------------- Messages ---------------------------------------- */

	private void uponPoint2PointMessage(Point2PointMessage point2PointMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received Point2Point Message: " + point2PointMessage.toString());

		if (receivedMessages.contains(point2PointMessage.getMid())) return;

		triggerNotification(new Deliver(point2PointMessage));
		receivedMessages.add(point2PointMessage.getMid());
	}

	private void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*--------------------------------- Notifications ---------------------------------------- */

	private void uponLookupReply(LookupReply reply, short protoID) {
		logger.info("Received Lookup Reply: " + reply.toString());

		Send send = messagesPendingLookupReply.get(reply.getMid());
		if (send == null) return;

		Point2PointMessage point2PointMessage = new Point2PointMessage(send, thisHost, PROTOCOL_ID);
		Host destinationHost = reply.getPeersIterator().next().getRight();
		sendMessage(point2PointMessage, destinationHost);

		messagesPendingLookupReply.remove(reply.getMid());
	}

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

    private void uponDHTInitialized(DHTInitializedNotification notification, short sourceProto) {
        logger.info("DHT is now initialized.");

		isDHTInitialized = true;
	    for (Send pendingMessage : messagesPendingLookup) {
		    uponSendRequest(pendingMessage, PROTOCOL_ID);
	    }
	    messagesPendingLookup.clear();
    }

}
