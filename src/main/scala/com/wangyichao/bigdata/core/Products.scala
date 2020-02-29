package com.wangyichao.bigdata.core

case class Products(name: String, price: Double, amount: Int) extends Ordered[Products] {

  override def compare(that: Products): Int = this.amount - that.amount
}
