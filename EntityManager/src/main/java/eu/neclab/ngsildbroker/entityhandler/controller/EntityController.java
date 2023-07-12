package eu.neclab.ngsildbroker.entityhandler.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLDService;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

/**
 *
 * @version 1.0
 * @date 10-Jul-2018
 */
@ApplicationScoped
@Path("/ngsi-ld/v1")
public class EntityController {// implements EntityHandlerInterface {

	private final static Logger logger = LoggerFactory.getLogger(EntityController.class);

	@Inject
	EntityService entityService;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@Inject
	JsonLDService ldService;

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/" rest endpoint.
	 *
	 * @param payload jsonld message
	 * @return ResponseEntity object
	 */
	@Path("/entities")
	@POST
	public Uni<RestResponse<Object>> createEntity(HttpServerRequest req, Map<String, Object> body) {
		noConcise(body);
		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					logger.debug("creating entity");
					return entityService.createEntity(HttpUtils.getTenant(req), tuple.getItem2(), tuple.getItem1())
							.onItem().transform(opResult -> {
								logger.debug("Done creating entity");
								return HttpUtils.generateCreateResult(opResult, AppConstants.ENTITES_URL);
							});
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 *
	 * @param entityId
	 * @param payload  json ld message
	 * @return ResponseEntity object
	 */

	@PATCH
	@Path("/entities/{entityId}/attrs")
	public Uni<RestResponse<Object>> updateEntity(HttpServerRequest req, @PathParam("entityId") String entityId,
			Map<String, Object> body) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		noConcise(body);
		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					logger.debug("patch attrs");
					return entityService
							.updateEntity(HttpUtils.getTenant(req), entityId, tuple.getItem2(), tuple.getItem1())
							.onItem().transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 *
	 * @param entityId
	 * @param payload  jsonld message
	 * @return ResponseEntity object
	 */

	@POST
	@Path("/entities/{entityId}/attrs")
	public Uni<RestResponse<Object>> appendEntity(HttpServerRequest req, @PathParam("entityId") String entityId,
			Map<String, Object> body, @QueryParam("options") String options) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		noConcise(body);
		boolean noOverwrite = options != null && options.contains(NGSIConstants.NO_OVERWRITE_OPTION);
		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					logger.debug("post attrs");
					return entityService.appendToEntity(HttpUtils.getTenant(req), entityId, tuple.getItem2(),
							noOverwrite, tuple.getItem1()).onItem().transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 *
	 * @param entityId
	 * @param payload
	 * @return
	 */
	@PATCH
	@Path("/entities/{entityId}/attrs/{attrId}")
	public Uni<RestResponse<Object>> partialUpdateAttribute(HttpServerRequest req,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrib, Map<String, Object> body) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		noConcise(body);

		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					String expAttrib = tuple.getItem1().expandIri(attrib, false, true, null, null);
					logger.debug("update entry :: started");
					return entityService.partialUpdateAttribute(HttpUtils.getTenant(req), entityId, expAttrib,
							tuple.getItem2(), tuple.getItem1()).onItem().transform(updateResult -> {
								logger.trace("update entry :: completed");
								return HttpUtils.generateUpdateResultResponse(updateResult);
							});
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 *
	 * @param entityId
	 * @param attrId
	 * @return
	 */

	@DELETE
	@Path("/entities/{entityId}/attrs/{attrId}")
	public Uni<RestResponse<Object>> deleteAttribute(HttpServerRequest request, @PathParam("entityId") String entityId,
			@PathParam("attrId") String attrId, @QueryParam("datasetId") String datasetId,
			@QueryParam("deleteAll") boolean deleteAll) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			String finalAttrId = context.expandIri(attrId, false, true, null, null);
			logger.trace("delete attribute :: started");
			return entityService
					.deleteAttribute(HttpUtils.getTenant(request), entityId, finalAttrId, datasetId, deleteAll, context)
					.onItem().transform(opResult -> {
						logger.trace("delete attribute :: completed");
						return HttpUtils.generateDeleteResult(opResult);

					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}" rest endpoint.
	 *
	 * @param entityId
	 * @return
	 */
	@DELETE
	@Path("/entities/{entityId}")
	public Uni<RestResponse<Object>> deleteEntity(HttpServerRequest request, @PathParam("entityId") String entityId) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			return entityService.deleteEntity(HttpUtils.getTenant(request), entityId, context).onItem()
					.transform(HttpUtils::generateDeleteResult);
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	public void noConcise(Object object) {
		noConcise(object, null, null);
	}

	private void noConcise(Object object, Map<String, Object> parentMap, String keyOfObject) {
		// Object is Map
		if (object instanceof Map<?, ?> map) {
			// Map have object but not type
			if (map.containsKey(NGSIConstants.OBJECT)) {
				((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.RELATIONSHIP);

			}
			// Map have value but not type
			if (map.containsKey(NGSIConstants.VALUE) && !map.containsKey(NGSIConstants.TYPE)) {
				// for GeoProperty
				if (map.get(NGSIConstants.VALUE) instanceof Map<?, ?> nestedMap
						&& (NGSIConstants.GEO_KEYWORDS.contains(nestedMap.get(NGSIConstants.TYPE))))
					((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.NGSI_LD_GEOPROPERTY_SHORT);
				else
					((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.PROPERTY);

			}
			// for GeoProperty
			if (map.containsKey(NGSIConstants.TYPE)
					&& (NGSIConstants.GEO_KEYWORDS.contains(map.get(NGSIConstants.TYPE)))
					&& !keyOfObject.equals(NGSIConstants.VALUE)) {
				Map<String, Object> newMap = new HashMap<>();
				newMap.put(NGSIConstants.TYPE, NGSIConstants.NGSI_LD_GEOPROPERTY_SHORT);
				newMap.put(NGSIConstants.VALUE, map);
				parentMap.put(keyOfObject, newMap);

			}

			// Iterate through every element of Map
			Object[] mapKeys = map.keySet().toArray();
			for (Object key : mapKeys) {
				if (!key.equals(NGSIConstants.ID) && !key.equals(NGSIConstants.TYPE)
						&& !key.equals(NGSIConstants.JSON_LD_CONTEXT)
						&& !key.equals(NGSIConstants.QUERY_PARAMETER_COORDINATES)
						&& !key.equals(NGSIConstants.QUERY_PARAMETER_OBSERVED_AT)
						&& !key.equals(NGSIConstants.INSTANCE_ID)
						&& !key.equals(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID) && !key.equals(NGSIConstants.OBJECT)
						&& !key.equals(NGSIConstants.VALUE) && !key.equals(NGSIConstants.SCOPE)
						&& !key.equals(NGSIConstants.QUERY_PARAMETER_UNIT_CODE)) {
					noConcise(map.get(key), (Map<String, Object>) map, key.toString());
				}
			}
		}
		// Object is List
		else if (object instanceof List<?> list) {
			for (int i = 0; i < list.size(); i++) {
				noConcise(list.get(i), null, null);
			}
		}
		// Object is String or Number value
		else if ((object instanceof String || object instanceof Number) && parentMap != null) {
			// if keyofobject is value then just need to convert double to int if possible
			if (keyOfObject != null && keyOfObject.equals(NGSIConstants.VALUE)) {
				parentMap.put(keyOfObject, HttpUtils.doubleToInt(object));
			} else {
				Map<String, Object> newMap = new HashMap<>();
				newMap.put(NGSIConstants.VALUE, HttpUtils.doubleToInt(object));
				newMap.put(NGSIConstants.TYPE, NGSIConstants.PROPERTY);
				parentMap.put(keyOfObject, newMap);
			}

		}
	}

	@PATCH
	@Path("/entities/{entityId}")
	public Uni<RestResponse<Object>> mergePatch(HttpServerRequest request, @PathParam("entityId") String entityId,
			Map<String, Object> body) {

		if (!entityId.equals(body.get(NGSIConstants.ID)) && body.get(NGSIConstants.ID) != null) {
			Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData, "Id can not be updated")));
		}

		noConcise(body);
		return HttpUtils.expandBody(request, body, AppConstants.MERGE_PATCH_REQUEST, ldService).onItem()
				.transformToUni(tuple -> {
					return entityService
							.mergePatch(HttpUtils.getTenant(request), entityId, tuple.getItem2(), tuple.getItem1())
							.onItem().transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}
	@Path("/entities/{entityId}")
	@PUT
	public Uni<RestResponse<Object>> replaceEntity(@PathParam("entityId") String entityId, HttpServerRequest request, Map<String, Object> body) {
		logger.debug("replacing entity");
		noConcise(body);

		return HttpUtils.expandBody(request, body, AppConstants.REPLACE_ENTITY_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					if(!body.get(NGSIConstants.ID).equals(entityId)){
						return 	Uni.createFrom().item(HttpUtils.handleControllerExceptions(
									new ResponseException(ErrorType.BadRequestData, "Id can not be updated")));
					}
					return entityService.replaceEntity(HttpUtils.getTenant(request), tuple.getItem2(), tuple.getItem1()).onItem()
							.transform(opResult -> {

								logger.debug("Done replacing entity");
								return HttpUtils.generateUpdateResultResponse(opResult);
							}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
				});
	}


	@Path("/entities/{entityId}/attrs/{attrId}")
	@PUT
	public Uni<RestResponse<Object>> replaceAttribute(@PathParam("attrId") String attrId,@PathParam("entityId") String entityId, HttpServerRequest request, Map<String, Object> body) {
		logger.debug("replacing Attrs");
		noConcise(body);
		return HttpUtils.expandBody(request, body, AppConstants.PARTIAL_UPDATE_REQUEST, ldService).onItem()
				.transformToUni(tuple -> {
					if(body.get(NGSIConstants.ID)!=null && !body.get(NGSIConstants.ID).equals(entityId)){
						return 	Uni.createFrom().item(HttpUtils.handleControllerExceptions(
								new ResponseException(ErrorType.BadRequestData, "Id can not be updated")));
					}
					String finalAttrId = tuple.getItem1().expandIri(attrId, false, true, null, null);
					return entityService.replaceAttribute(HttpUtils.getTenant(request), tuple.getItem2(), tuple.getItem1(),entityId,finalAttrId).onItem()
							.transform(opResult -> {
								logger.debug("Done replacing entity");
								return HttpUtils.generateUpdateResultResponse(opResult);
							}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
				});
	}
}
