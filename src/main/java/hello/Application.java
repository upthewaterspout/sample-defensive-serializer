package hello;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Data;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.gemfire.cache.config.EnableGemfireCaching;
import org.springframework.data.gemfire.config.annotation.EnableCachingDefinedRegions;
import org.springframework.data.gemfire.config.annotation.EnablePdx;
import org.springframework.data.gemfire.config.annotation.PeerCacheApplication;
import org.springframework.data.gemfire.mapping.GemfireMappingContext;
import org.springframework.data.gemfire.mapping.MappingPdxSerializer;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.pdx.PdxWriter;

@SpringBootApplication
@PeerCacheApplication
@EnableCachingDefinedRegions(clientRegionShortcut = ClientRegionShortcut.LOCAL)
@EnableGemfireCaching
@EnablePdx(serializerBeanName = "myCustomMappingPdxSerializer")
@SuppressWarnings("unused")
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	ApplicationRunner runner() {
		return args -> {
			CircularObject circularObject = new CircularObject();
			circularObject.reference = circularObject;
			DataSerializer.writeObject(circularObject, new DataOutputStream(new ByteArrayOutputStream()));
			DataSerializer.writeObject(circularObject, new DataOutputStream(new ByteArrayOutputStream()));
		};
	}

	@Bean
	MappingPdxSerializer myCustomMappingPdxSerializer() {
        	return new DefensiveMappingPdxSerializer();
	}

	private static class DefensiveMappingPdxSerializer extends MappingPdxSerializer {
		ThreadLocal<Set<Object>> objectsInStack = ThreadLocal.withInitial(() -> Collections.newSetFromMap(new IdentityHashMap<>()));

		public DefensiveMappingPdxSerializer() {
			super(new GemfireMappingContext(), new DefaultConversionService());
		}

		@Override
		public boolean toData(Object value, PdxWriter pdxWriter) {
			if(!objectsInStack.get().add(value)) {
				List<? extends Class<?>> classesInStack = objectsInStack.get().stream()
								.map(Object::getClass)
								.collect(Collectors.toList());
				throw new IllegalStateException(
						String.format("Circular references found while serializing instance of %s. Classes in stack are %s.", value.getClass(),
								classesInStack));
			}
			try {
				return super.toData(value, pdxWriter);
			} finally {
				objectsInStack.get().remove(value);
			}
		}
	}

	@Data
	public static class CircularObject {
	  public Object reference;

	  @Override
	  public String toString() {
	    return getClass().getName();
	  }
	}
}
