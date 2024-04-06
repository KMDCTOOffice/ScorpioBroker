package eu.neclab.ngsildbroker.commons.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import javax.sql.DataSource;
import com.google.common.collect.Maps;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import eu.neclab.ngsildbroker.commons.utils.QuarkusConfigDump;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.quarkus.arc.Arc;
import io.quarkus.flyway.runtime.FlywayContainer;
import io.quarkus.flyway.runtime.FlywayContainerProducer;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.SslMode;
import io.vertx.sqlclient.PoolOptions;

@Singleton
public class ClientManager {

	Logger logger = LoggerFactory.getLogger(ClientManager.class);

	@Inject
	QuarkusConfigDump quarkusConfigDump;

	// Create own pgPool from quarkus.datasource properties
	// @Inject
	PgPool pgClient;

	@Inject
	AgroalDataSource writerDataSource;

	@Inject
	Vertx vertx;

	@ConfigProperty(name = "quarkus.datasource.reactive.url")
	String reactiveDsDefaultUrl;
	@ConfigProperty(name = "quarkus.datasource.jdbc.url")
	String jdbcBaseUrl;
	@ConfigProperty(name = "quarkus.datasource.jdbc.driver")
	String jdbcDriver;
	@ConfigProperty(name = "quarkus.datasource.username")
	String username;
	@ConfigProperty(name = "quarkus.datasource.password")
	String password;

	@ConfigProperty(name = "quarkus.datasource.reactive.postgresql.ssl-mode")
	String reactiveDsPostgresqlSslMode;
	@ConfigProperty(name = "quarkus.datasource.reactive.trust-all")
	boolean reactiveDsPostgresqlSslTrustAll;

	@ConfigProperty(name = "quarkus.datasource.reactive.shared")
	boolean reactiveDsShared;
	@ConfigProperty(name = "quarkus.datasource.reactive.cache-prepared-statements")
	boolean reactiveDsCachePreparedStatements;

	@ConfigProperty(name = "quarkus.datasource.reactive.max-size")
	int reactiveMaxSize;
	@ConfigProperty(name = "quarkus.datasource.reactive.idle-timeout")
	Duration idleTime;

	@ConfigProperty(name = "quarkus.transaction-manager.default-transaction-timeout")
	Duration connectionTime;

	@ConfigProperty(name = "pool.minsize")
	int minsize;
	@ConfigProperty(name = "pool.maxsize")
	int maxsize;
	@ConfigProperty(name = "pool.initialSize")
	int initialSize;

	protected ConcurrentMap<String, Uni<PgPool>> tenant2Client = Maps.newConcurrentMap();

	@PostConstruct
	void loadTenantClients() throws URISyntaxException {
		logger.warn("Using custom reactive datasource Postgresql connection pool!");

		logger.info("Base jdbc url: {}", new URI(jdbcBaseUrl));
		logger.info("Default reactive jdbc url: {}, sslmode: {}", new URI(reactiveDsDefaultUrl), reactiveDsPostgresqlSslMode);

		try {
			pgClient = createPgPool("scorpio_default_pool", reactiveDsDefaultUrl);
			testPgPool(pgClient, "scorpio_default_pool");
			tenant2Client.put(AppConstants.INTERNAL_NULL_KEY, Uni.createFrom().item(pgClient));
			createAllTenantConnections(pgClient);
		} catch (Exception e) {
			logger.error("Error connectiong to database: ", reactiveDsDefaultUrl, e);
			e.printStackTrace();
			throw e;
		}
	}

	private String getClientPoolName(String tenant) {
		return "scorpio_tenant_" + tenant + "_pool";
	}

	private void createAllTenantConnections(PgPool pool) {
		pool.query("SELECT tennant_id, database_name FROM public.tenant").execute().onItem().invoke(r -> {
			r.forEach(tr -> {
				String tenant = tr.getString("tennant_id");
				String dbName = tr.getString("database_name");
				String databaseUrl = DBUtil.databaseURLFromPostgresJdbcUrl(reactiveDsDefaultUrl, dbName);
				logger.info("Creating client for tenant '{}'", tenant);
				AgroalDataSource clientDatasource;
				try {
					logger.info("Running database migration for tenant '{}' on database '{}'", tenant, dbName);
					clientDatasource = createDatasource(tenant, dbName);
					flywayMigrate(clientDatasource, tenant);
					clientDatasource.close();
					logger.info("Database migration for tenant '{}' finished", tenant);
				} catch (SQLException e) {
					logger.error("Database migration for tenant '{}' error: {}", tenant, e);
					e.printStackTrace();
				}

				String poolName = getClientPoolName(tenant);
				PgPool clientPool = createPgPool(poolName, databaseUrl);
				testPgPool(clientPool, poolName);
				tenant2Client.put(tenant, Uni.createFrom().item(clientPool));
				logger.info("Done Creating client for tenant '{}'", tenant);
			});
		});
	}

	public Uni<PgPool> getClient(String tenant, boolean create) {
		if (tenant == null) {
			return tenant2Client.get(AppConstants.INTERNAL_NULL_KEY);
		}
		Uni<PgPool> result = tenant2Client.get(tenant);
		if (result == null) {
			result = getTenant(tenant, create);
			return result.onItem().transformToUni(pgClient->{
                return tenant2Client.put(tenant, Uni.createFrom().item(pgClient));
			});
		}
		return result;
	}

	private PgPool createPgPool(String poolName, String databaseUrl) {
		logger.info("Creating reactive datasource pool '{}'; database: {}, sslmode: {}", poolName, databaseUrl, reactiveDsPostgresqlSslMode);
		PgConnectOptions connectOptions = PgConnectOptions.fromUri(databaseUrl)
			.setUser(username)
			.setPassword(password)
			.setPipeliningLimit(100)
			.setCachePreparedStatements(reactiveDsCachePreparedStatements)
			.setSslMode(SslMode.valueOf(reactiveDsPostgresqlSslMode.toUpperCase()))
			.setTrustAll(reactiveDsPostgresqlSslTrustAll);
		PoolOptions poolOptions = new PoolOptions()
			.setName(poolName)
			.setShared(reactiveDsShared)
			.setMaxSize(reactiveMaxSize)
			.setIdleTimeout((int) idleTime.getSeconds())
			.setIdleTimeoutUnit(TimeUnit.SECONDS)
			.setConnectionTimeout((int) connectionTime.getSeconds())
			.setConnectionTimeoutUnit(TimeUnit.SECONDS);
		return PgPool.pool(vertx, connectOptions, poolOptions);
	}

	private void testPgPool(PgPool pool, String poolName) {
		logger.info("Reactive datasource pool {} test query: SELECT 1", poolName);
		// pool.query("SELECT 1").execute().onItem().invoke(r -> {
		// 	logger.info("Reactive datasource pool {} test query {}", poolName, r.size()==1?"OK":"ERROR");
		// });
		int cnt = pool.query("SELECT 1").execute().await().atMost(Duration.ofSeconds(5)).rowCount();
		logger.info("Reactive datasource pool {} test query {}", poolName, cnt==1?"OK":"ERROR");
	}

	private Uni<PgPool> getTenant(String tenant, boolean createDB) {
		return determineTargetDataSource(tenant, createDB).onItem().transformToUni(Unchecked.function(finalDataBase -> {
			String databaseUrl = DBUtil.databaseURLFromPostgresJdbcUrl(reactiveDsDefaultUrl, finalDataBase);
			logger.info("Creating reactive datasource pool for tenant '{}' with database '{}'", tenant, finalDataBase);
			Uni<PgPool> result = Uni.createFrom().item(createPgPool(getClientPoolName(tenant), databaseUrl));
			tenant2Client.put(tenant, result);
			return result;
		}));
	}

	public Uni<String> determineTargetDataSource(String tenantidvalue, boolean createDB) {
		return createDataSourceForTenantId(tenantidvalue, createDB).onItem().transform(tenantDataSource -> {
			logger.info("Running database migration for tenant '{}' on database '{}'", tenantidvalue, tenantDataSource.getItem2());
			flywayMigrate(tenantDataSource.getItem1(), tenantidvalue);
			tenantDataSource.getItem1().close();
			return tenantDataSource.getItem2();
		});
	}

	private Uni<Tuple2<AgroalDataSource, String>> createDataSourceForTenantId(String tenantidvalue, boolean createDB) {
		return findDataBaseNameByTenantId(tenantidvalue, createDB).onItem()
				.transform(Unchecked.function(tenantDatabaseName -> {
					AgroalDataSource agroaldataSource = createDatasource(tenantidvalue, tenantDatabaseName);
					return Tuple2.of(agroaldataSource, tenantDatabaseName);
				}));
	}


	private AgroalDataSource createDatasource(String tenant, String tenantDatabaseName) throws SQLException {
		// TODO this needs to be from the config not hardcoded!!!
		String tenantJdbcURL = "jdbc:" + DBUtil.databaseURLFromPostgresJdbcUrl(jdbcBaseUrl, tenantDatabaseName);
		logger.info("Creating datasource for tenant '{}' with jdbc url: {}", tenant, tenantJdbcURL);
		AgroalDataSourceConfigurationSupplier configuration = new AgroalDataSourceConfigurationSupplier()
				.dataSourceImplementation(DataSourceImplementation.AGROAL).metricsEnabled(false)
				.connectionPoolConfiguration(
						cp -> cp.minSize(minsize).maxSize(maxsize).initialSize(initialSize)
								.connectionFactoryConfiguration(cf -> cf.jdbcUrl(tenantJdbcURL)
										.connectionProviderClassName(jdbcDriver)
										.autoCommit(false)
										.principal(new NamePrincipal(username))
										.credential(new SimplePassword(password))));
		return AgroalDataSource.from(configuration);
	}

	public ConcurrentMap<String, Uni<PgPool>> getAllClients() {
		return tenant2Client;
	}

	public Uni<String> findDataBaseNameByTenantId(String tenant, boolean create) {
		String databasename = "ngb" + tenant.hashCode();
		String databasenameWithoutHash = "ngb" + tenant;
		return pgClient.preparedQuery("SELECT datname FROM pg_database where datname = $1 OR datname = $2")
				.execute(Tuple.of(databasename, databasenameWithoutHash)).onItem().transformToUni(pgRowSet -> {
					if (pgRowSet.size() == 0) {
						if (create) {
							return pgClient.preparedQuery("create database \"" + databasename + "\"").execute().onItem()
									.transformToUni(t -> {
										return storeTenantdata(tenant, databasename).onItem()
												.transform(t2 -> databasename);
									});
						} else {
							return Uni.createFrom().failure(
									new ResponseException(ErrorType.TenantNotFound, tenant + " tenant was not found"));
						}
					} else {
						return pgClient.preparedQuery("SELECT datname FROM pg_database where datname = $1")
								.execute(Tuple.of(databasenameWithoutHash)).onItem().transformToUni(rowSet -> {
									if (rowSet.size() != 0) {
										return Uni.createFrom().item(databasenameWithoutHash);
									} else
										return Uni.createFrom().item(databasename);
								});
					}
				});
	}

	private Uni<Void> storeTenantdata(String tenantidvalue, String databasename) {
		return pgClient.preparedQuery(
				"INSERT INTO tenant (tenant_id, database_name) VALUES ($1, $2) ON CONFLICT(tenant_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id")
				.execute(Tuple.of(tenantidvalue, databasename)).onItem().ignore().andContinueWithNull();
	}

	private boolean flywayMigrate(DataSource tenantDataSource, String tenant) {
        FlywayContainerProducer flywayProducer = Arc.container().instance(FlywayContainerProducer.class).get();
        FlywayContainer flywayContainer = flywayProducer.createFlyway(tenantDataSource, "scoprpio_tenant_" + tenant + "_datasource", true, true);
        Flyway flyway = flywayContainer.getFlyway();
		try {
			flyway.migrate();
		} catch (Exception e) {
			logger.warn("failed to create tenant database attempting repair", e);
			try {
				flyway.repair();
				flyway.migrate();
			} catch (Exception e1) {
				logger.error("repair failed", e);
				return false;
			}
		}
		return true;
	}

}
