package protocols.broadcast.flood;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.flood.messages.FloodMessage;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class FloodBroadcast extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(FloodBroadcast.class);

	//Protocol information, to register in babel
	public static final String PROTOCOL_NAME = "FloodBroadcast";
	public static final short PROTOCOL_ID = 200;

	private final Host myself; //My own address/port
	private final Set<Host> neighbours; //My known neighbours (a.k.a peers the membership protocol told me about)
	private final Set<UUID> received; //Set of received messages (since we do not want to deliver the same msg twice)

	//We can only start sending messages after the membership protocol informed us that the channel is ready
	private boolean channelReady;

	public FloodBroadcast(Properties properties, Host myself) throws HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = myself;
		neighbours = new HashSet<>();
		received = new HashSet<>();
		channelReady = false;

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

		/*--------------------- Register Notification Handlers ----------------------------- */
		subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
		subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
	}

	@Override
	public void init(Properties props) {
		//Nothing to do here, we just wait for event from the membership or the application
	}

	/*--------------------------------- Requests ---------------------------------------- */
	private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
		if (!channelReady) return; //Ideally we would buffer this message to transmit when the channel is ready :)

		//Create the message object.
		FloodMessage msg = new FloodMessage(request.getMsgId(), request.getSender(), sourceProto, request.getMsg());

		//Call the same handler as when receiving a new FloodMessage (since the logic is the same)
		uponFloodMessage(msg, myself, getProtoId(), -1);
	}

	/*--------------------------------- Messages ---------------------------------------- */
	private void uponFloodMessage(FloodMessage msg, Host from, short sourceProto, int channelId) {
		logger.trace("Received {} from {}", msg, from);
		//If we already received it once, do nothing (or we would end up with a nasty infinite loop)
		if (received.add(msg.getMid())) {
			//Deliver the message to the application (even if it came from it)
			triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));

			//Simply send the message to every known neighbour (who will then do the same)
			neighbours.forEach(host -> {
				if (!host.equals(from)) {
					logger.trace("Sent {} to {}", msg, host);
					sendMessage(msg, host);
				}
			});
		}
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
	                         Throwable throwable, int channelId) {
		//If a message fails to be sent, for whatever reason, log the message and the reason
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*--------------------------------- Notifications ---------------------------------------- */

	//When the membership protocol notifies of a new neighbour (or leaving one) simply update my list of neighbours.
	private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			neighbours.add(h);
			logger.info("New neighbour: " + h);
		}
	}

	private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			neighbours.remove(h);
			logger.info("Neighbour down: " + h);
		}
	}

	//Upon receiving the channelId from the membership, register our own callbacks and serializers
	private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
		int cId = notification.getChannelId();
		// Allows this protocol to receive events from this channel.
		registerSharedChannel(cId);
		/*---------------------- Register Message Serializers ---------------------- */
		registerMessageSerializer(cId, FloodMessage.MSG_ID, FloodMessage.serializer);
		/*---------------------- Register Message Handlers -------------------------- */
		try {
			registerMessageHandler(cId, FloodMessage.MSG_ID, this::uponFloodMessage, this::uponMsgFail);
		} catch (HandlerRegistrationException e) {
			logger.error("Error registering message handler: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
		//Now we can start sending messages
		channelReady = true;
	}

}
