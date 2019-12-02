package volcano

import static volcano.db.Type.BOOLEAN
import static volcano.db.Type.DOUBLE
import static volcano.db.Type.INT
import static volcano.db.Type.STRING

import java.sql.Connection
import java.sql.DriverManager

import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification
import volcano.db.Database
import volcano.db.Row
import volcano.db.Table

class Driver extends Specification {

  @Rule
  TemporaryFolder tempFolder

  Database db

  def setup() {
    def departmentTableNames = ['id', 'name', 'abbreviation', 'description', 'is_stem']
    def departmentTableTypes = [INT, STRING, STRING, STRING, BOOLEAN]
    def departmentData = rows([
        [1, 'Computer Science', 'CMSC', 'Computer science department', true],
        [2, 'Mathematics', 'MATH', 'Mathematics department', true],
        [3, 'Physics', 'PHYS', 'Physics Department', true],
        [4, 'Statistics', 'STAT', 'Statistics Department', true],
        [5, 'Political Science', 'GVPT', 'Political Science Department', false]
    ])

    def courseTableNames = ['id', 'title', 'department', 'professor', 'course_number', 'max_enrollment', 'avg_gpa']
    def courseTableTypes = [INT, STRING, STRING, STRING, INT, INT, DOUBLE]
    def courseData = rows([
        [8, 'Ethics in Politics', 'GVPT', 'Grover', 202, 100, 4.0],
        [3, 'Algorithms', 'CMSC', 'Knuth', 351, 50, 2.8],
        [1, 'Calculus 1', 'MATH', 'Newton', 140, 100, 3.2],
        [2, 'Calculus 2', 'MATH', 'Newton', 141, 100, 2.5],
        [4, 'Physics 1', 'PHYS', 'Newton', 101, 100, 2.0],
        [5, 'Physics 2', 'PHYS', 'Schrödinger', 201, 75, 3.5],
        [6, 'Statistics 1', 'STAT', 'Newton', 101, 100, 3.8],
        [9, 'International Relations', 'GVPT', 'Lang', 301, 200, 3.9],
        [7, 'Statistics 2', 'STAT', 'Schrödinger', 201, 20, 3.1],
    ])

    def departmentTable = new Table('department', departmentTableNames, departmentTableTypes, departmentData)
    def courseTable = new Table('course', courseTableNames, courseTableTypes, courseData)
    db = new Database([course: courseTable, department: departmentTable])
    populateH2Database(courseData, departmentData)
  }

  def 'main test'() {
    given:
    def query = """
select department.name, 
       max(course.avg_gpa) 
from   course 
       join department 
         on course.department = department.abbreviation 
group  by department.name 
order  by department.name
"""

    expect:
    def result = new QueryEngine(db, 'localhost').executeQuery(query)
    assert result == queryH2Database(query)
    println()
    println()
    println("================ RESULTS ================")
    result.each {
      println("[" + it + "]")
    }
    println("=========================================")
  }

  private void populateH2Database(List<Row> courseData, List<Row> departmentData) {
    Connection c
    try {
      c = DriverManager.getConnection("jdbc:h2:~/$tempFolder.root", "sa", "")
      c.createStatement().execute("create table course (id int, title varchar, department varchar, professor varchar, course_number int, max_enrollment int, avg_gpa double)")
      c.createStatement().execute("create table department (id int, name varchar, abbreviation varchar, description varchar, is_stem boolean)")
      def insertCourseValues = "insert into course values ${courseData.collect { '(' + it + ')' }.join(",")}"
      def insertDeptValues = "insert into department values ${departmentData.collect { '(' + it + ')' }.join(",")}"
      c.createStatement().execute(insertCourseValues)
      c.createStatement().execute(insertDeptValues)
    }
    finally {
      c?.close()
    }
  }

  private List<Row> queryH2Database(String query) {
    Connection c
    try {
      c = DriverManager.getConnection("jdbc:h2:~/$tempFolder.root", "sa", "")
      def resultSet = c.createStatement().executeQuery(query)
      def resultRows = []
      def metadata = resultSet.getMetaData()
      while (resultSet.next()) {
        resultRows.add(new Row(
            (1..metadata.columnCount).collect {
              resultSet.getObject(it)
            }
        )
        )
      }
      return resultRows
    }
    finally {
      c?.close()
    }
  }

  private static List<Row> rows(List<List> rrs) {
    rrs.collect { it as Row }
  }
}
