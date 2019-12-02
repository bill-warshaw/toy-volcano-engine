package volcano

import static volcano.db.Type.BOOLEAN
import static volcano.db.Type.DOUBLE
import static volcano.db.Type.INT
import static volcano.db.Type.STRING

import spock.lang.Specification
import volcano.db.Database
import volcano.db.Row
import volcano.db.Table

class Driver extends Specification {

  def 'main test'() {
    given:
    def tableOneNames = ['customer_id', 'customer_name', 'customer_totalspend', 'customer_happy']
    def tableOneTypes = [INT, STRING, DOUBLE, BOOLEAN]
    def tableOne = new Table('customers', tableOneNames, tableOneTypes, dataT1)
    def tableTwoNames = ['id', 'n', 'x']
    def tableTwoTypes = [INT, STRING, INT]
    def tableTwo = new Table('t2', tableTwoNames, tableTwoTypes, dataT2)
    Database db = new Database([customers: tableOne, t2: tableTwo])

    expect:
//    new QueryEngine(db).executeQuery('select distinct customer_id,customer_name from customers where customer_id > 1 order by customer_name asc, customer_id desc limit 2') == []
//    new QueryEngine(db).executeQuery("select customer_id, avg(customer_totalspend) from customers group by customer_id") == []
    new QueryEngine(db).executeQuery("select customers.customer_id, t2.n, t2.x from customers join t2 on customer_id") == []
  }

  private static List<Row> getDataT1() {
    rows([
        [1, 'bill', 3.14 as double, true],
        [2, 'peets', 6.83 as double, false],
        [3, 'aaa', 1.2 as double, true],
        [3, 'ggg', 2.3 as double, false],
        [4, 'aaa', 2.3 as double, false],
        [4, 'aaa', 3.4 as double, true]
    ])
  }

  private static List<Row> getDataT2() {
    rows([
        [1, 'bill', 111],
        [2, 'peets', 222],
        [3, 'aaa', 333]
    ])
  }

  private static List<Row> rows(List<List> rrs) {
    rrs.collect { it as Row }
  }
}
