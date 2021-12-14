package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:23
 */
public class Subscription extends Query {

	private String description;
	private Long expiresAt;
	private URI id;
	private String subscriptionName;
	private NotificationParam notification;
	private String status;
	private Integer throttling = 0;
	private Integer timeInterval = 0;
	private String type;
	private Boolean internal = false;
	private QueryTerm queryTerm;
	private boolean isActive = true;

	public Subscription() {
		super();
	}

	public Subscription(Map<String, String> customFlags, List<String> attributeNames, List<EntityInfo> entities,
			String ldContext, LDGeoQuery ldGeoQuery, String ldQuery, LDTemporalQuery ldTempQuery,
			List<URI> requestorList, String description, Long expires, URI id, String subscriptionName,
			NotificationParam notification, String status, Integer throttling, Integer timeInterval, String type) {
		super(customFlags, attributeNames, entities, ldContext, ldGeoQuery, ldQuery, ldTempQuery, requestorList);
		this.description = description;
		this.expiresAt = expiresAt;
		this.id = id;
		this.subscriptionName = subscriptionName;
		this.notification = notification;
		this.status = status;
		this.throttling = throttling;
		this.timeInterval = timeInterval;
		this.type = type;
	}

	public Boolean isInternal() {
		return internal;
	}

	public void setInternal(Boolean internal) {
		this.internal = internal;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Long getExpiresAt() {
		return expiresAt;
	}

	public void setExpiresAt(Long expiresAt) {
		this.expiresAt = expiresAt;
	}

	public URI getId() {
		return id;
	}

	public void setId(URI id) {
		this.id = id;
	}

	public String getSubscriptionName() {
		return subscriptionName;
	}

	public void setSubscriptionName(String subscriptionName) {
		this.subscriptionName = subscriptionName;
	}

	public NotificationParam getNotification() {
		return notification;
	}

	public void setNotification(NotificationParam notification) {
		this.notification = notification;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Integer getThrottling() {
		return throttling;
	}

	public void setThrottling(Integer throttling) {
		this.throttling = throttling;
	}

	public Integer getTimeInterval() {
		return timeInterval;
	}

	public void setTimeInterval(Integer timeInterval) {
		this.timeInterval = timeInterval;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public QueryTerm getQueryTerm() {
		return queryTerm;
	}

	public void setQueryTerm(QueryTerm queryTerm) {
		this.queryTerm = queryTerm;
	}

	public void finalize() throws Throwable {

	}

	public boolean isActive() {
		return isActive;
	}

	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}

	@Override
	public String toString() {
		return "Subscription [description=" + description + ", expiresAt=" + expiresAt + ", id=" + id + ", subscriptionName=" + subscriptionName
				+ ", notification=" + notification + ", status=" + status + ", throttling=" + throttling
				+ ", timeInterval=" + timeInterval + ", type=" + type + ", internal=" + internal + ", queryTerm="
				+ queryTerm + ", attributeNames=" + attributeNames + ", entities=" + entities + ", ldContext="
				+ ldContext + ", ldGeoQuery=" + ldGeoQuery + ", ldQuery=" + ldQuery + ", ldTempQuery=" + ldTempQuery
				+ ", requestorList=" + requestorList + ", customFlags=" + customFlags + "]";
	}

}