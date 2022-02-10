/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;

import java.time.OffsetDateTime;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: update migration description in the class name
public class V0_35_28_001__AddActorCatalogMetadataColumns extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      V0_35_28_001__AddActorCatalogMetadataColumns.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> modifiedAt =
        DSL.field("modified_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    ctx.alterTable("actor_catalog")
        .addIfNotExists(modifiedAt).execute();
    ctx.alterTable("actor_catalog_fetch_event")
        .addIfNotExists(createdAt).execute();
    ctx.alterTable("actor_catalog_fetch_event")
        .addIfNotExists(modifiedAt).execute();
  }

}
