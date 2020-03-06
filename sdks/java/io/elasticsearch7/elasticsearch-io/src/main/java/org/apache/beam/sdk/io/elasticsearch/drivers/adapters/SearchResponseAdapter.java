package org.apache.beam.sdk.io.elasticsearch.drivers.adapters;

import java.util.ListIterator;

public interface SearchResponseAdapter {
  String getScrollId();

  ListIterator<? extends SearchHitAdapter> iterator();
}
