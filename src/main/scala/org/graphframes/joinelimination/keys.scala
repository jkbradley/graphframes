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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.expressions.NamedExpression

/**
 * A key constraint on the output of a [[LogicalPlan]].
 */
sealed abstract class Key {
  def transformAttribute(rule: PartialFunction[Attribute, Attribute]): Key
  def resolved: Boolean
}

/**
 * Declares that the values of `attr` are unique. The `keyId` is used to enable [[ForeignKey]]s to
 * reference this key regardless of attribute rewrites, so it should be preserved when transforming
 * `attr` whenever the transformation would preserve referential integrity.
 */
case class UniqueKey(
    attr: Attribute,
    keyId: ExprId = NamedExpression.newExprId) extends Key {
  override def transformAttribute(rule: PartialFunction[Attribute, Attribute]): Key =
    UniqueKey(rule.applyOrElse(attr, identity[Attribute]), keyId)

  override def resolved: Boolean = attr.resolved
}

/**
 * Declares that the values of `attr` reference the unique key with id `referencedKeyId`.
 */
case class ForeignKey(
    attr: Attribute,
    referencedKeyId: ExprId) extends Key {
  override def transformAttribute(rule: PartialFunction[Attribute, Attribute]): Key =
    ForeignKey(rule.applyOrElse(attr, identity[Attribute]), referencedKeyId)

  override def resolved: Boolean = attr.resolved
}
