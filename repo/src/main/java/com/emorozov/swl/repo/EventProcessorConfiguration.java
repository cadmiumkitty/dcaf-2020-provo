package com.emorozov.swl.repo;

import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Namespace;
import org.openprovenance.prov.model.ProvFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class EventProcessorConfiguration {

  public static final String SWL_PREFIX = "swl";

  public static final String SWL_NAMESPACE = "http://semanticweblondon.com/";

  @Bean
  public ProvFactory provFactory() {
    return InteropFramework.getDefaultFactory();
  }

  @Bean
  public Namespace namespace() {
    Namespace ns = new Namespace();
    ns.addKnownNamespaces();
    ns.register(SWL_PREFIX, SWL_NAMESPACE);
    return ns;
  }

  @Bean
  public InteropFramework interopFramework() {
    return new InteropFramework();
  }
}