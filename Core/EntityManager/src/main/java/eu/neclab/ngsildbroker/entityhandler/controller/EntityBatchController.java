package eu.neclab.ngsildbroker.entityhandler.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.BatchFailure;
import eu.neclab.ngsildbroker.commons.datatypes.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.ControllerFunctions;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")
public class EntityBatchController {

	@Autowired
	EntityService entityService;

	@Value("${batchoperations.maxnumber.create:-1}")
	int maxCreateBatch;
	@Value("${batchoperations.maxnumber.update:-1}")
	int maxUpdateBatch;
	@Value("${batchoperations.maxnumber.upsert:-1}")
	int maxUpsertBatch;
	@Value("${batchoperations.maxnumber.delete:-1}")
	int maxDeleteBatch;

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@PostMapping("/create")
	public ResponseEntity<byte[]> createMultiple(ServerHttpRequest request, @RequestBody String payload) {
		return ControllerFunctions.createMultiple(entityService, request, payload, maxCreateBatch);
	}

	@PostMapping("/upsert")
	public ResponseEntity<byte[]> upsertMultiple(ServerHttpRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {
		return ControllerFunctions.upsertMultiple(entityService, request, payload, options, maxCreateBatch);
	}

	@PostMapping("/update")
	public ResponseEntity<byte[]> updateMultiple(ServerHttpRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {
		return ControllerFunctions.updateMultiple(entityService, request, payload, maxUpdateBatch, options);
	}

	@PostMapping("/delete")
	public ResponseEntity<byte[]> deleteMultiple(ServerHttpRequest request, @RequestBody String payload) {
		return ControllerFunctions.deleteMultiple(entityService, request, payload);
	}

}
