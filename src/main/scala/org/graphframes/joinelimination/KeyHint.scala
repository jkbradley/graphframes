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

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode

object KeyHint {
  /**
   * Returns the unique and foreign key constraints that will hold for the output of `plan`.
   * Specific plan nodes are handled specially to introduce or propagate keys.
   */
  def collectKeys(plan: LogicalPlan): Seq[Key] = plan match {
    case KeyHint(newKeys, child) => newKeys ++ collectKeys(child)

    case Project(projectList, child) =>
      val aliasMap = AttributeMap(projectList.collect {
        case a @ Alias(old: AttributeReference, _) => (old, a.toAttribute)
        case r: AttributeReference => (r, r)
      })
      collectKeys(child).collect {
        case UniqueKey(attr, keyId) if aliasMap.contains(attr) =>
          UniqueKey(aliasMap(attr), keyId)
        case ForeignKey(attr, referencedAttr) if aliasMap.contains(attr) =>
          ForeignKey(aliasMap(attr), referencedAttr)
      }

    case Join(left, right, joinType, _) =>
      // TODO: try to propagate unique keys as well as foreign keys
      def fk(keys: Seq[Key]): Seq[ForeignKey] = keys.collect { case k: ForeignKey => k }
      joinType match {
        case LeftSemi | LeftOuter => fk(collectKeys(left))
        case RightOuter => fk(collectKeys(right))
        case _ => fk(collectKeys(left) ++ collectKeys(right))
      }

    case Subquery(_, child) => collectKeys(child)

    case _ => Seq.empty
  }
}

/**
 * A hint to the optimizer that the given key constraints hold for the output of the child plan.
 */
case class KeyHint(newKeys: Seq[Key], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override lazy val resolved: Boolean = newKeys.forall(_.resolved) && childrenResolved

  /** Overridden here to apply `rule` to the keys. */
  override def transformExpressionsDown(
      rule: PartialFunction[Expression, Expression]): this.type = {
    KeyHint(newKeys.map(_.transformAttribute(rule.andThen(_.asInstanceOf[Attribute]))), child)
      .asInstanceOf[this.type]
  }

  /** Overridden here to apply `rule` to the keys. */
  override def transformExpressionsUp(
      rule: PartialFunction[Expression, Expression]): this.type = {
    KeyHint(newKeys.map(_.transformAttribute(rule.andThen(_.asInstanceOf[Attribute]))), child)
      .asInstanceOf[this.type]
  }
}
