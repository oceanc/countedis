package com.github.oceanc.countedis.net

import _root_.java.util.{ArrayList => JArrayList, Date}

import scala.beans.BeanProperty

/**
 * @author chengyang
 */
object MockClasses {

  case class Shoe(color: String, logo: String, weight: Int)

  case class Person(name: String, age: Int, born: java.util.Date, shoe: Shoe)

  case class GroupBySeq(id: String, leader: Person, employee: Seq[Person])

  case class GroupByMap(id: String, leader: Person, employee: Map[Int, Person])

  case class GroupBySet(id: String, leader: Person, employee: Set[Person])

  val shoe = Shoe("black", "nike", 20)
  val gary = Person("gary", 40, new Date, null)
  val jack = Person("jack", 30, new Date, shoe)
  val groupSeq = GroupBySeq("group1", gary, List(jack))
  val groupMap = GroupByMap("group1", gary, Map(1 -> jack))
  val groupSet = GroupBySet("group1", gary, Set(jack))




  // follow java bean specification
  class Shirt(@BeanProperty var color: String, @BeanProperty var logo: String, @BeanProperty var weight: Int, @BeanProperty var produceDate: java.util.Date) {
    def this() = this(null, null, 0, null)
    def this(color: String, logo: String, weight: Int) = this(color, logo, weight, new Date)
  }

  // follow java bean specification
  class Shop(@BeanProperty var name: String, @BeanProperty var owner: String, @BeanProperty var productions: java.util.List[Shirt]) {
    def this() = this(null, null, null)
  }

  // java bean Shirt equivalent
  case class ShirtScala(color: String, logo: String, weight: Int, produceDate: java.util.Date = new Date)

  //java bean Shop equivalent
  case class ShopScala(name: String, owner: String, productions: Seq[ShirtScala])

  val list = new JArrayList[Shirt](3)
  val shirtA = new Shirt("blue", "adidas", 20)
  val shirtB = new Shirt("blue", "adidas", 20)
  val shirtC = new Shirt("blue", "adidas", 20)
  val shop = new Shop("CocaCola", "tom", list)
  list.add(shirtA)
  list.add(shirtB)
  list.add(shirtC)
}