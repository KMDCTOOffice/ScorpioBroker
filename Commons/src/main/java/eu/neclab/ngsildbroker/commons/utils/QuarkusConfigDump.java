package eu.neclab.ngsildbroker.commons.utils;

import java.util.stream.StreamSupport;

import jakarta.inject.Singleton;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class QuarkusConfigDump {

	@PostConstruct
	void init() {
		System.out.println("==================== "+this.getClass().getName() + " init/@PostConstruct ====================");
		Thread.dumpStack();
		// dumpConfig();
	}

	void onStart(@Observes StartupEvent event) {
		System.out.println("==================== "+this.getClass().getName() + " startup/@StartupEvent ====================");
		// Thread.dumpStack();
		dumpConfig();
	}

	void dumpConfig() {
		Config conf = ConfigProvider.getConfig();
		StringBuffer sb = new StringBuffer();

		StreamSupport.stream(conf.getPropertyNames().spliterator(), false).filter(n -> { return n.startsWith("quarkus."); }).sorted().forEach(p -> {
			var c = conf.getConfigValue(p);
			sb.append("\t").append(c.getName()).append(": ").append(c.getValue()).append(" (").append(c.getSourceName()).append(")\n");
		});
		System.out.println(sb.toString());
	}
}
