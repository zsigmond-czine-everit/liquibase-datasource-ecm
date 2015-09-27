/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.persistence.liquibase.datasource.ecm.internal;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.sql.DataSource;

import org.everit.osgi.liquibase.bundle.LiquibaseOSGiUtil;
import org.everit.persistence.liquibase.ecm.LiquibaseService;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.Constants;
import org.osgi.framework.Filter;
import org.osgi.framework.ServiceRegistration;
import org.osgi.framework.wiring.BundleCapability;
import org.osgi.framework.wiring.BundleWiring;
import org.osgi.service.log.LogService;
import org.osgi.util.tracker.BundleTracker;

/**
 * Bundle tracker that handle bundle changes to Liquibase Capability.
 */
public class LiquibaseCapabilityTracker extends BundleTracker<Bundle> {

  private final String componentPid;

  private ServiceRegistration<DataSource> dataSourceSR;

  private final Filter filter;

  private final LiquibaseService liquibaseService;

  private final LogService logService;

  private final LinkedHashMap<Bundle, BundleCapability> matchingBundles =
      new LinkedHashMap<Bundle, BundleCapability>();

  private final String schemaExpression;

  private Bundle selectedBundle;

  private final DataSource wrappedDataSource;

  private final Map<String, Object> wrappedDataSourceServiceProperties;

  /**
   * Constructor.
   */
  public LiquibaseCapabilityTracker(final BundleContext context, final String schemaExpression,
      final LiquibaseService liquibaseService, final DataSource wrappedDataSource,
      final Map<String, Object> wrappedDataSourceServiceProperties, final String componentPid,
      final LogService logService) {
    super(context, Bundle.ACTIVE, null);
    this.logService = logService;
    this.liquibaseService = liquibaseService;
    filter = LiquibaseOSGiUtil.createFilterForLiquibaseCapabilityAttributes(schemaExpression);
    this.wrappedDataSource = wrappedDataSource;
    this.wrappedDataSourceServiceProperties = wrappedDataSourceServiceProperties;
    this.componentPid = componentPid;
    this.schemaExpression = schemaExpression;
  }

  @Override
  public Bundle addingBundle(final Bundle bundle, final BundleEvent event) {
    handleBundleChange(bundle);
    return bundle;
  }

  private void dropBundle(final Bundle bundle) {
    matchingBundles.remove(bundle);
    if (bundle.equals(selectedBundle)) {
      selectedBundle = null;
      dataSourceSR.unregister();
      dataSourceSR = null;
    }
  }

  private BundleCapability findMatchingCapabilityInBundle(final Bundle bundle) {
    BundleWiring bundleWiring = bundle.adapt(BundleWiring.class);
    List<BundleCapability> capabilities =
        bundleWiring.getCapabilities(LiquibaseOSGiUtil.LIQUIBASE_CAPABILITY_NS);
    Iterator<BundleCapability> iterator = capabilities.iterator();
    BundleCapability matchingCapability = null;
    while ((matchingCapability == null) && iterator.hasNext()) {
      BundleCapability capability = iterator.next();
      Map<String, Object> attributes = capability.getAttributes();
      if (filter.matches(attributes)) {
        matchingCapability = capability;
      }
    }
    return matchingCapability;
  }

  private synchronized void handleBundleChange(final Bundle bundle) {
    dropBundle(bundle);

    BundleCapability matchingCapability = findMatchingCapabilityInBundle(bundle);
    if (matchingCapability != null) {
      matchingBundles.put(bundle, matchingCapability);
    }
    if (selectedBundle == null) {
      selectBundleIfNecessary();
    }
  }

  @Override
  public void modifiedBundle(final Bundle bundle, final BundleEvent event, final Bundle object) {
    handleBundleChange(bundle);
  }

  @Override
  public synchronized void removedBundle(final Bundle bundle, final BundleEvent event,
      final Bundle object) {
    dropBundle(bundle);
    selectBundleIfNecessary();
  }

  private void selectBundleIfNecessary() {
    if (selectedBundle != null) {
      return;
    }
    Set<Entry<Bundle, BundleCapability>> entries = matchingBundles.entrySet();
    Iterator<Entry<Bundle, BundleCapability>> iterator = entries.iterator();
    boolean selected = false;
    while (iterator.hasNext() && !selected) {
      Entry<Bundle, BundleCapability> entry = iterator.next();
      BundleCapability bundleCapability = entry.getValue();
      Map<String, Object> attributes = bundleCapability.getAttributes();
      String resourceName = (String) attributes.get(LiquibaseOSGiUtil.ATTR_SCHEMA_RESOURCE);
      Bundle bundle = entry.getKey();
      try {
        liquibaseService.process(wrappedDataSource, bundle, resourceName);
        selectedBundle = bundle;
        selected = true;
        logService.log(LogService.LOG_INFO,
            "Successfully migrated database from schema [" + bundle.toString()
                + " - " + resourceName + "], registering DataSource");

        Hashtable<String, Object> serviceProps =
            new Hashtable<String, Object>(wrappedDataSourceServiceProperties);

        Object wrappedDSServiceId = wrappedDataSourceServiceProperties.get(Constants.SERVICE_ID);
        if (wrappedDSServiceId != null) {
          serviceProps.put("wrappedDataSource." + Constants.SERVICE_ID, wrappedDSServiceId);
        }

        Object wrappedDSServicePid = wrappedDataSourceServiceProperties.get(Constants.SERVICE_PID);
        if (wrappedDSServicePid != null) {
          serviceProps.put("wrappedDataSource." + Constants.SERVICE_PID, wrappedDSServicePid);
        }
        serviceProps.put(Constants.SERVICE_PID, componentPid);
        serviceProps.put("liquibase.schema.bundle.id", bundle.getBundleId());
        serviceProps.put("liquibase.schema.bundle.symbolicName", bundle.getSymbolicName());
        serviceProps.put("liquibase.schema.bundle.version", bundle.getVersion().toString());
        Object schemaName = attributes.get(LiquibaseOSGiUtil.ATTR_SCHEMA_NAME);
        serviceProps.put("liquibase.schema.name", schemaName);
        serviceProps.put("liquibase.schema.expression", schemaExpression);
        serviceProps.put("liquibase.schema.resource", resourceName);

        dataSourceSR =
            super.context.registerService(DataSource.class, wrappedDataSource, serviceProps);
      } catch (RuntimeException e) {
        logService.log(LogService.LOG_ERROR,
            "Could not update database with schema file " + resourceName
                + " of bundle " + bundle.toString(),
            e);
      }
    }
  }

}
