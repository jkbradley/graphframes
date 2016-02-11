/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graphframes.joinelimination

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object JoinEliminationHelper {
  private def rules: Seq[Rule[LogicalPlan]] = (
    ResolveForeignKeyReferences +:
    Seq.fill(10)(JoinElimination) :+
    RemoveKeyHints)

  def registerRules(sqlContext: SQLContext): Unit = {
    if (!sqlContext.experimental.extraOptimizations.containsSlice(rules)) {
      sqlContext.experimental.extraOptimizations ++= rules
    }
  }

  implicit class JoinEliminationImplicits(df: DataFrame) {
    /**
     * Declares that the values of the given column are unique.
     */
    def uniqueKey(col: String): DataFrame =
      new DataFrame(
        df.sqlContext,
        KeyHintCollapsing(KeyHint(List(UniqueKey(UnresolvedAttribute(col))), df.queryExecution.logical)))

    /**
     * Declares that the values of the given column reference a unique column from another
     * [[DataFrame]]. The referenced column must be declared as a unique key within the referenced
     * [[DataFrame]].
     */
    def foreignKey(
        col: String, referencedDF: DataFrame, referencedCol: String): DataFrame =
      new DataFrame(
        df.sqlContext,
        KeyHintCollapsing(
          KeyHint(
            List(ForeignKey(UnresolvedAttribute(col), UnresolvedAttribute(referencedCol))),
          df.queryExecution.logical)))
  }
}
