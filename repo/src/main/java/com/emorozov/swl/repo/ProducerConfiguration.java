package com.emorozov.swl.repo;

import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Namespace;
import org.openprovenance.prov.model.ProvFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProducerConfiguration {

  @Bean
  public ProvFactory provFactory() {
    return InteropFramework.getDefaultFactory();
  }

  @Bean
  public Namespace namespace() {
    Namespace ns = new Namespace();
    ns.addKnownNamespaces();
    ns.register("swl", "https://semanticweblondon.com/");
    return ns;
  }

  @Bean
  public InteropFramework interopFramework() {
    return new InteropFramework();
  }
}