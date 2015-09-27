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

import java.util.Map;

import javax.sql.DataSource;

import org.everit.osgi.ecm.annotation.Activate;
import org.everit.osgi.ecm.annotation.Component;
import org.everit.osgi.ecm.annotation.ConfigurationPolicy;
import org.everit.osgi.ecm.annotation.Deactivate;
import org.everit.osgi.ecm.annotation.ServiceRef;
import org.everit.osgi.ecm.annotation.attribute.StringAttribute;
import org.everit.osgi.ecm.annotation.attribute.StringAttributes;
import org.everit.osgi.ecm.component.ConfigurationException;
import org.everit.osgi.ecm.component.ServiceHolder;
import org.everit.osgi.ecm.extender.ECMExtenderConstants;
import org.everit.persistence.liquibase.datasource.ecm.LiquibaseDataSourceConstants;
import org.everit.persistence.liquibase.ecm.LiquibaseService;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.service.log.LogService;

import aQute.bnd.annotation.headers.ProvideCapability;

/**
 * ECM based configurable component that process liquibase schemas.
 */
@Component(componentId = LiquibaseDataSourceConstants.SERVICE_PID,
    configurationPolicy = ConfigurationPolicy.FACTORY, label = "DataSource (Liquibase) (Everit)",
    description = "A component that makes it possible to call Liquibase functionality during "
        + "activating bundles that rely on database schema.")
@ProvideCapability(ns = ECMExtenderConstants.CAPABILITY_NS_COMPONENT,
    value = ECMExtenderConstants.CAPABILITY_ATTR_CLASS + "=${@class}")
@StringAttributes({
    @StringAttribute(attributeId = Constants.SERVICE_DESCRIPTION,
        defaultValue = "Default Liquibase DataSource", label = "Service Description",
        description = "The description of this component configuration. It is used to easily "
            + "identify the service registered by this component.") })
public class LiquibaseDataSourceComponent {

  public static final int PRIORITY_01_SCHEMA_EXPRESSION = 1;

  public static final int PRIORITY_02_EMBEDDED_DATASOURCE = 2;

  public static final int PRIORITY_03_LIQUIBASE_SERVICE = 3;

  public static final int PRIORITY_04_LOG_SERVICE = 4;

  private DataSource embeddedDataSource;

  private Map<String, Object> embeddedDataSourceProperties;

  private LiquibaseService liquibaseService;

  private LogService logService;

  private String schemaExpression;

  private LiquibaseCapabilityTracker tracker;

  /**
   * Component activator method.
   */
  @Activate
  public void activate(final BundleContext context, final Map<String, Object> componentProperties) {
    if (schemaExpression == null) {
      throw new ConfigurationException("schemaExpression must be defined");
    }
    Object servicePidValue = componentProperties.get(Constants.SERVICE_PID);
    String servicePid = String.valueOf(servicePidValue);

    tracker = new LiquibaseCapabilityTracker(context, schemaExpression, liquibaseService,
        embeddedDataSource, embeddedDataSourceProperties, servicePid, logService);
    tracker.open();
  }

  @Deactivate
  public void deactivate() {
    tracker.close();
    tracker = null;
  }

  @ServiceRef(attributeId = LiquibaseDataSourceConstants.ATTR_EMBEDDED_DATASOURCE_TARGET,
      defaultValue = "", attributePriority = PRIORITY_02_EMBEDDED_DATASOURCE,
      label = "Embedd DataSource filter",
      description = "OSGi filter expression to reference the DataSource service that will be re-"
          + "registered after the database schema is processed by Liquibase.")
  public void setEmbeddedDataSource(final ServiceHolder<DataSource> serviceHolder) {
    embeddedDataSource = serviceHolder.getService();
    embeddedDataSourceProperties = serviceHolder.getAttributes();
  }

  @ServiceRef(attributeId = LiquibaseDataSourceConstants.ATTR_LIQUIBASE_SERVICE_TARGET,
      defaultValue = "", attributePriority = PRIORITY_03_LIQUIBASE_SERVICE,
      label = "LiquibaseService filter",
      description = "The OSGI filter expression of LiquibaseService.")
  public void setLiquibaseService(final LiquibaseService liquibaseService) {
    this.liquibaseService = liquibaseService;
  }

  @ServiceRef(attributeId = LiquibaseDataSourceConstants.ATTR_LOG_SERVICE_TARGET, defaultValue = "",
      attributePriority = PRIORITY_04_LOG_SERVICE, label = "LogService filter",
      description = "The OSGi filter expression of LogService")
  public void setLogService(final LogService logService) {
    this.logService = logService;
  }

  @StringAttribute(attributeId = LiquibaseDataSourceConstants.ATTR_SCHEMA_EXPRESSION,
      priority = LiquibaseDataSourceComponent.PRIORITY_01_SCHEMA_EXPRESSION,
      label = "Schema expression",
      description = "An expression that references the schema to reference a capability of a "
          + "provider bundle. The syntax of the expression is schemaName[;filter:=(expression)] "
          + "where the name of the schema is required. A filter can be defined as well in case "
          + "the same schema is provided by multiple bundles or if the same bundle provides "
          + "the same schema name from different resources. E.g. If we have the "
          + "\"Provide-Capability: liquibase.schema;name=myApp;resource=/META-INF/changelog.xml;"
          + "version=2.0.0\", the value of this property can be "
          + "\"myApp;filter:=(version>=2)\". ")
  public void setSchemaExpression(final String schemaExpression) {
    this.schemaExpression = schemaExpression;
  }

}
