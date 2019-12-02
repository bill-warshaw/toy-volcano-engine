package volcano

import static volcano.db.Type.BOOLEAN
import static volcano.db.Type.INT
import static volcano.db.Type.STRING

import spock.lang.Specification
import volcano.db.Database
import volcano.db.Row
import volcano.db.Table
import volcano.db.Type

class Driver extends Specification {

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
    def courseTableTypes = [INT, STRING, STRING, STRING, INT, INT, Type.DOUBLE]
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
  }

  def 'main test'() {
    given:
    def query = """
SELECT Max(course.avg_gpa), 
       department.NAME 
FROM   course 
       JOIN department 
         ON course.department = department.abbreviation 
GROUP  BY department.NAME 
"""

    expect:
    new QueryEngine(db).executeQuery(query) == []
  }

  private static List<Row> rows(List<List> rrs) {
    rrs.collect { it as Row }
  }
}
