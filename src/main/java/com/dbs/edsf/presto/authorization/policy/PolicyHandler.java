package com.dbs.edsf.presto.authorization.policy;

import com.dbs.edsf.presto.metrics.Metric;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import java.util.Optional;
import java.util.Set;

public interface PolicyHandler {
   @Metric(
      key = "remote.access_check.response_time",
      type = Metric.TYPE.TIMER
   )
   void checkAccess(Identity var1, String var2, String var3, String var4) throws AccessDeniedException;

   @Metric(
      key = "remote.transform_query.response_time",
      type = Metric.TYPE.TIMER
   )
   String getTransformedQuery(Identity var1, String var2, Optional<String> var3, Optional<String> var4) throws AccessDeniedException;

   @Metric(
      key = "remote.inline_view.response_time",
      type = Metric.TYPE.TIMER
   )
   String getInlineView(Identity var1, CatalogSchemaTableName var2, Optional<Set<String>> var3) throws AccessDeniedException;

   @Metric(
      key = "remote.trusted_user.response_time",
      type = Metric.TYPE.TIMER
   )
   void validateTrustedUser(String var1, Optional<String> var2, Optional<String> var3, Optional<String> var4) throws AccessDeniedException;
}
