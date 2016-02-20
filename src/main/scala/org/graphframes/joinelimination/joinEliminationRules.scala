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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Combines two adjacent [[KeyHint]]s into one by merging their key lists.
 */
object KeyHintCollapsing extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case KeyHint(keys1, KeyHint(keys2, child)) =>
      KeyHint((keys1 ++ keys2).distinct, child)
  }
}

/**
 * Eliminates keyed equi-joins when followed by a [[Project]] that only keeps columns from one side.
 */
object JoinElimination extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case CanEliminateUniqueKeyOuterJoin(outer, projectList) =>
      Project(projectList, outer)
    case CanEliminateReferentialIntegrityJoin(parent, child, primaryForeignMap, projectList) =>
      Project(substituteParentForChild(projectList, parent, primaryForeignMap), child)
  }

  /**
   * In the given expressions, substitute all references to parent columns with references to the
   * corresponding child columns. The `primaryForeignMap` contains these equivalences, extracted
   * from the equality join expressions.
   */
  private def substituteParentForChild(
      expressions: Seq[NamedExpression],
      parent: LogicalPlan,
      primaryForeignMap: AttributeMap[Attribute])
    : Seq[NamedExpression] = {
    expressions.map(_.transform {
      case a: Attribute =>
        if (parent.outputSet.contains(a)) Alias(primaryForeignMap(a), a.name)(a.exprId)
        else a
    }.asInstanceOf[NamedExpression])
  }

}

/**
 * Removes [[KeyHint]]s from the plan to avoid interfering with other rules.
 */
object RemoveKeyHints extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case KeyHint(_, child) => child
  }
}
