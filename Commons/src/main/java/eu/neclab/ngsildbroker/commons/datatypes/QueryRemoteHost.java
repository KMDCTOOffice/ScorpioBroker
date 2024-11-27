package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.mutiny.core.MultiMap;

public class QueryRemoteHost {
	String host;
	String tenant;
	MultiMap headers;
	String cSourceId;
	boolean canDoQuery;
	boolean canDoBatchQuery;
	boolean canDoRetrieve;
	int regMode;
	List<Tuple3<String, String, String>> idsAndTypesAndIdPattern = Lists.newArrayList();
	Map<String, String> queryParams;
	boolean canDoEntityMap;
	boolean canDoZip;
	String entityMapToken;
	Context context;
	ViaHeaders viaHeaders;

	public QueryRemoteHost(String host, String tenant, MultiMap headers, String cSourceId, boolean canDoQuery,
			boolean canDoBatchQuery, boolean canDoRetrieve, int regMode,
			List<Tuple3<String, String, String>> idsAndTypesAndIdPattern, Map<String, String> queryParams,
			boolean canDoEntityMap, boolean canDoZip, String entityMapToken, ViaHeaders viaHeaders) {
		this.host = host;
		this.tenant = tenant;
		this.headers = headers;
		this.cSourceId = cSourceId;
		this.canDoBatchQuery = canDoBatchQuery;
		this.canDoQuery = canDoQuery;
		this.canDoRetrieve = canDoRetrieve;
		this.regMode = regMode;
		this.canDoEntityMap = canDoEntityMap;
		this.canDoZip = canDoZip;
		this.entityMapToken = entityMapToken;
		this.queryParams = queryParams;
		this.idsAndTypesAndIdPattern = idsAndTypesAndIdPattern;
		this.viaHeaders = viaHeaders;
	}

	public QueryRemoteHost copyFor414Handle(String id, String type, String idPattern) {
		Tuple3<String, String, String> tmpTuple = Tuple3.of(id, type, idPattern);
		List<Tuple3<String, String, String>> idAndTypesAndIdPatternEntry = new ArrayList<>(1);
		idAndTypesAndIdPatternEntry.add(tmpTuple);
		return new QueryRemoteHost(host, tenant, headers, cSourceId, canDoQuery, canDoBatchQuery, canDoRetrieve,
				regMode, idAndTypesAndIdPatternEntry, queryParams, canDoEntityMap, canDoZip, entityMapToken,
				viaHeaders);
	}

	public String host() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String tenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public MultiMap headers() {
		return headers;
	}

	public void setHeaders(MultiMap headers) {
		this.headers = headers;
	}

	public String cSourceId() {
		return cSourceId;
	}

	public void setcSourceId(String cSourceId) {
		this.cSourceId = cSourceId;
	}

	public int regMode() {
		return regMode;
	}

	public void setRegMode(int regMode) {
		this.regMode = regMode;
	}

	public Map<String, String> getQueryParam() {
		return queryParams;
	}

	public void setQueryParam(Map<String, String> queryParams) {
		this.queryParams = queryParams;
	}

	public boolean isCanDoQuery() {
		return canDoQuery;
	}

	public void setCanDoQuery(boolean canDoQuery) {
		this.canDoQuery = canDoQuery;
	}

	public boolean isCanDoBatchQuery() {
		return canDoBatchQuery;
	}

	public void setCanDoBatchQuery(boolean canDoBatchQuery) {
		this.canDoBatchQuery = canDoBatchQuery;
	}

	public boolean isCanDoRetrieve() {
		return canDoRetrieve;
	}

	public void setCanDoRetrieve(boolean canDoRetrieve) {
		this.canDoRetrieve = canDoRetrieve;
	}

	public boolean isCanDoEntityMap() {
		return canDoEntityMap;
	}

	public boolean canDoEntityMap() {
		return canDoEntityMap;
	}

	public void setCanDoEntityMap(boolean canDoEntityMap) {
		this.canDoEntityMap = canDoEntityMap;
	}

	public boolean canDoZip() {
		return canDoZip;
	}

	public void setCanDoZip(boolean canDoZip) {
		this.canDoZip = canDoZip;
	}

	public String entityMapToken() {
		return entityMapToken;
	}

	public void setEntityMapToken(String entityMapToken) {
		this.entityMapToken = entityMapToken;
	}

	public Context context() {
		return context;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public void setParamsFromNext(String nextLink) {
		String pureLink = nextLink.substring(1, nextLink.length() - 12);

		String params = pureLink.substring(pureLink.indexOf('?'));
		int index = params.indexOf('&', 0);
		int lastIndex = 0;
		int equalIdx;
		queryParams.clear();
		String paramPart;
		while (index != -1) {
			paramPart = params.substring(lastIndex, index);
			equalIdx = paramPart.indexOf('=', lastIndex);
			if (equalIdx == -1) {
				queryParams.put(paramPart, "true");
			} else {
				queryParams.put(paramPart.substring(0, equalIdx), paramPart.substring(equalIdx, paramPart.length()));
			}
			lastIndex = index;
			index = params.indexOf('&', lastIndex);
		}
		paramPart = params.substring(lastIndex, index);
		equalIdx = paramPart.indexOf('=', lastIndex);
		if (equalIdx == -1) {
			queryParams.put(paramPart, "true");
		} else {
			queryParams.put(paramPart.substring(0, equalIdx), paramPart.substring(equalIdx, paramPart.length()));
		}

	}

	public List<Tuple3<String, String, String>> getIdsAndTypesAndIdPattern() {
		return idsAndTypesAndIdPattern;
	}

	public void addIdsAndTypesAndIdPattern(Tuple3<String, String, String> idsAndTypesAndIdPattern) {
		this.idsAndTypesAndIdPattern.add(idsAndTypesAndIdPattern);
	}

	public static QueryRemoteHost fromRegEntry(RemoteHost remoteHost, boolean canDoIdQuery, boolean canDoZip) {
		return new QueryRemoteHost(remoteHost.host(), remoteHost.tenant(), remoteHost.headers(), remoteHost.cSourceId(),
				remoteHost.canDoSingleOp(), remoteHost.canDoBatchOp(), remoteHost.canDoBatchOp(), remoteHost.regMode(),
				Lists.newArrayList(), Maps.newHashMap(), canDoIdQuery, canDoZip, null, null);
	}

	public ViaHeaders getViaHeaders() {
		return viaHeaders;
	}

	public void setViaHeaders(ViaHeaders viaHeaders) {
		this.viaHeaders = viaHeaders;
	}

	public static QueryRemoteHost fromRegEntry(RegistrationEntry regEntry) {
		RemoteHost remoteHost = regEntry.host();
		QueryRemoteHost result = new QueryRemoteHost(remoteHost.host(), remoteHost.tenant(), remoteHost.headers(),
				remoteHost.cSourceId(), regEntry.queryEntity(), regEntry.queryBatch(), regEntry.retrieveEntity(),
				remoteHost.regMode(), Lists.newArrayList(), Maps.newHashMap(), regEntry.queryEntityMap(), false, null,
				null);
		result.setContext(regEntry.context());
		return result;
	}

	@Override
	public String toString() {
		return "QueryRemoteHost [host=" + host + ", tenant=" + tenant + ", headers=" + headers + ", cSourceId="
				+ cSourceId + ", canDoQuery=" + canDoQuery + ", canDoBatchQuery=" + canDoBatchQuery + ", canDoRetrieve="
				+ canDoRetrieve + ", regMode=" + regMode + ", idsAndTypesAndIdPattern=" + idsAndTypesAndIdPattern
				+ ", queryParams=" + queryParams + ", canDoEntityMap=" + canDoEntityMap + ", canDoZip=" + canDoZip
				+ ", entityMapToken=" + entityMapToken + ", context=" + context + ", viaHeaders=" + viaHeaders + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(cSourceId, canDoBatchQuery, canDoEntityMap, canDoQuery, canDoRetrieve, canDoZip, context,
				entityMapToken, headers, host, idsAndTypesAndIdPattern, queryParams, regMode, tenant, viaHeaders);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueryRemoteHost other = (QueryRemoteHost) obj;
		return Objects.equals(cSourceId, other.cSourceId) && canDoBatchQuery == other.canDoBatchQuery
				&& canDoEntityMap == other.canDoEntityMap && canDoQuery == other.canDoQuery
				&& canDoRetrieve == other.canDoRetrieve && canDoZip == other.canDoZip
				&& Objects.equals(context, other.context) && Objects.equals(entityMapToken, other.entityMapToken)
				&& Objects.equals(headers, other.headers) && Objects.equals(host, other.host)
				&& Objects.equals(idsAndTypesAndIdPattern, other.idsAndTypesAndIdPattern)
				&& Objects.equals(queryParams, other.queryParams) && regMode == other.regMode
				&& Objects.equals(tenant, other.tenant) && Objects.equals(viaHeaders, other.viaHeaders);
	}
	
	

}
