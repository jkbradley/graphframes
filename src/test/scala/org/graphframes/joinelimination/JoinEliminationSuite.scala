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

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.scalatest.FunSuite

class JoinEliminationSuite extends FunSuite {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", FixedPoint(10), EliminateSubQueries) ::
      Batch("JoinElimination", Once, JoinElimination) :: Nil
  }

  def getUniqueKeyId(plan: LogicalPlan, attr: Attribute): ExprId = {
    KeyHint.collectKeys(plan).collectFirst {
      case UniqueKey(attr2, keyId) if attr semanticEquals attr2 => keyId
    }.get
  }

  val customer = {
    val r = LocalRelation('customerId.int.notNull, 'customerName.string)
    KeyHint(List(UniqueKey(r.output(0))), r)
  }
  val employee = {
    val r = LocalRelation('employeeId.int.notNull, 'employeeName.string)
    KeyHint(List(UniqueKey(r.output(0))), r)
  }
  val order = {
    val r = LocalRelation(
      'orderId.int.notNull, 'o_customerId.int.notNull, 'o_employeeId.int)
    KeyHint(List(
      UniqueKey(r.output(0)),
      ForeignKey(r.output(1), getUniqueKeyId(customer, customer.output(0))),
      ForeignKey(r.output(2), getUniqueKeyId(employee, employee.output(0)))), r)
  }
  val bannedCustomer = {
    val r = LocalRelation('bannedCustomerName.string.notNull)
    KeyHint(List(UniqueKey(r.output(0))), r)
  }

  def checkJoinEliminated(
      base: LogicalPlan,
      join: LogicalPlan => LogicalPlan,
      project: LogicalPlan => LogicalPlan,
      projectAfterElimination: LogicalPlan => LogicalPlan): Unit = {
    val query = project(join(base))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = projectAfterElimination(base).analyze
    comparePlans(optimized, correctAnswer)
  }

  def checkJoinEliminated(
      base: LogicalPlan,
      join: LogicalPlan => LogicalPlan,
      project: LogicalPlan => LogicalPlan): Unit = {
    checkJoinEliminated(base, join, project, project)
  }

  def checkJoinNotEliminated(
      base: LogicalPlan,
      join: LogicalPlan => LogicalPlan,
      project: LogicalPlan => LogicalPlan): Unit = {
    val query = project(join(base))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("eliminate unique key left outer join") {
    checkJoinEliminated(
      customer,
      _.join(bannedCustomer, LeftOuter, Some('customerName === 'bannedCustomerName)),
      _.select('customerId, 'customerName))
  }

  test("do not eliminate unique key inner join") {
    checkJoinNotEliminated(
      customer,
      _.join(bannedCustomer, Inner, Some('customerName === 'bannedCustomerName)),
      _.select('customerId, 'customerName))
  }

  test("do not eliminate unique key full outer join") {
    checkJoinNotEliminated(
      customer,
      _.join(bannedCustomer, FullOuter, Some('customerName === 'bannedCustomerName)),
      _.select('customerId, 'customerName))
  }

  test("do not eliminate referential integrity inner join where foreign key is nullable") {
    checkJoinNotEliminated(
      order,
      _.join(employee, Inner, Some('employeeId === 'o_employeeId)),
      _.select('orderId, 'employeeId))
  }

  test("eliminate referential integrity inner join when foreign key is not null") {
    checkJoinEliminated(
      order,
      _.join(customer, Inner, Some('customerId === 'o_customerId)),
      _.select('orderId, 'customerId),
      _.select('orderId, 'o_customerId.as('customerId)))
  }

  test("eliminate referential integrity left/right outer join when foreign key is not null") {
    checkJoinEliminated(
      order,
      _.join(customer, LeftOuter, Some('customerId === 'o_customerId)),
      _.select('orderId, 'customerId),
      _.select('orderId, 'o_customerId.as('customerId)))

    checkJoinEliminated(
      order,
      customer.join(_, RightOuter, Some('customerId === 'o_customerId)),
      _.select('orderId, 'customerId),
      _.select('orderId, 'o_customerId.as('customerId)))
  }

  test("do not eliminate referential integrity full outer join") {
    checkJoinNotEliminated(
      order,
      _.join(customer, FullOuter, Some('customerId === 'o_customerId)),
      _.select('orderId, 'customerId))
  }

  test("eliminate referential integrity outer join despite alias") {
    checkJoinEliminated(
      order,
      _.join(customer.select('customerId.as('customerId_alias), 'customerName),
        LeftOuter, Some('customerId_alias === 'o_customerId)),
      _.select('orderId, 'customerId_alias),
      _.select('orderId, 'o_customerId.as('customerId_alias)))
  }

  // From org.apache.spark.sql.catalyst.plans.PlanTest:
  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  protected def normalizeExprIds(plan: LogicalPlan) = {
    plan transformAllExpressions {
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(0))
    }
  }

  /** Fails the test if the two plans do not match */
  protected def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan) {
    import org.apache.spark.sql.catalyst.util.sideBySide
    val normalized1 = normalizeExprIds(plan1)
    val normalized2 = normalizeExprIds(plan2)
    if (normalized1 != normalized2) {
      fail(
        s"""
          |== FAIL: Plans do not match ===
          |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
         """.stripMargin)
    }
  }
}
