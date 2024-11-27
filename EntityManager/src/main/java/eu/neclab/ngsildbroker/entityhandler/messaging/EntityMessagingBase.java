package eu.neclab.ngsildbroker.entityhandler.messaging;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.inject.Inject;

public abstract class EntityMessagingBase {

	private final static Logger logger = LoggerFactory.getLogger(EntityMessagingBase.class);

	@Inject
	EntityService entityService;

	@Inject
	Vertx vertx;

	@Inject
	ObjectMapper objectMapper;

	



//	CollectMessageListener collectListenerRegistry = new CollectMessageListener() {
//
//		@Override
//		public void collected(String byteMessage) {
//			BaseRequest message;
//			try {
//				message = objectMapper.readValue(byteMessage, BaseRequest.class);
//			} catch (IOException e) {
//				logger.error("failed to read sync message", e);
//				return;
//			}
//			baseHandleCsource(message).runSubscriptionOn(executor).subscribe()
//					.with(v -> logger.debug("done handling registry"));
//		}
//	};

	public Uni<Void> handleCsourceRaw(String byteMessage) {
		//collector.collect(byteMessage, collectListenerRegistry);
		CSourceBaseRequest message;
		try {
			message = objectMapper.readValue(byteMessage, CSourceBaseRequest.class);
			return baseHandleCsource(message);
		} catch (IOException e) {
			logger.error("failed to read sync message", e);
			return Uni.createFrom().voidItem();
		}

		
	}

	public Uni<Void> baseHandleCsource(CSourceBaseRequest message) {
		logger.debug("entity manager got called for csource: " + message.getId());
		return entityService.handleRegistryChange(message);
	}


}
