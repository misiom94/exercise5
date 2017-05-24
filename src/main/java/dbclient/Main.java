package dbclient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.*;



public class Main {
	
	private static Connection con = null;
	private static PreparedStatement statemet = null;  
	
	private static Logger log = Logger.getLogger(Main.class);
	
	//Statements
	
	//DROPING TABLES 
	
	private static final String DROP_TABLE_STUDENT = "DROP TABLE Student IF EXISTS CASCADE";
	private static final String DROP_TABLE_FACULTY = "DROP TABLE Faculty IF EXISTS CASCADE";
	private static final String DROP_TABLE_CLASS = "DROP TABLE Class IF EXISTS CASCADE";
	private static final String DROP_TABLE_ENROLLMENT = "DROP TABLE Enrollment IF EXISTS CASCADE";
	
	//CREATING TABLES
	
	private static final String CREATE_TABLE_STUDENT =
			"CREATE TABLE Student( " +
            "pkey INT NOT NULL," +
            "name VARCHAR(50) NOT NULL," +
            "sex VARCHAR(6) NOT NULL," +
            "age INT NOT NULL," +
            "level INT NOT NULL," +
            "PRIMARY KEY (pkey) );";
	
	private static final String CREATE_TABLE_FACULTY = 
			"CREATE TABLE Faculty( " +
            "pkey INT NOT NULL," +
            "name VARCHAR(50) NOT NULL," +
        	"PRIMARY KEY (pkey) );";
	
	private static final String CREATE_TABLE_CLASS =
			"CREATE TABLE Class( " +
            "pkey INT NOT NULL," +
            "name VARCHAR(50) NOT NULL," +
            "fkey_faculty INT NOT NULL," +
            "PRIMARY KEY (pkey),"+
            "FOREIGN KEY (fkey_faculty) REFERENCES Faculty (pkey));";
	
	private static final String CREATE_TABLE_ENROLLMENT = 
            "CREATE TABLE Enrollment( " +
            "fkey_student INT NOT NULL," +
            "fkey_class INT NOT NULL," +
            "FOREIGN KEY (fkey_class) REFERENCES Class (pkey),"+
            "FOREIGN KEY (fkey_student) REFERENCES Student (pkey));";
	
	//INSERTING DATA TO STUDENT
	
	private static final String STUDENT_INSERT_VAL1 = 
            "INSERT INTO Student "+
            "VALUES (1, 'John Smith', 'male', 23, 2);";
	
	private static final String STUDENT_INSERT_VAL2 = 
            "INSERT INTO Student "+
            "VALUES (2, 'Rebecca Milson', 'female', 27, 3);";
	private static final String STUDENT_INSERT_VAL3 = 
            "INSERT INTO Student "+
            "VALUES (3, 'George Heartbreaker', 'male', 19, 1);";
	private static final String STUDENT_INSERT_VAL4 = 
            "INSERT INTO Student "+
            "VALUES (4, 'Deepika Chopra', 'female', 25, 3);";
	
	//INSERTING DATA TO FACULTY
	private static final String FACULTY_INSERT_VAL1 = 
            "INSERT INTO Faculty "+
            "VALUES (100, 'Engineering');";
	private static final String FACULTY_INSERT_VAL2 = 
            "INSERT INTO Faculty "+
            "VALUES (101, 'Philosophy');";
	private static final String FACULTY_INSERT_VAL3 = 
            "INSERT INTO Faculty "+
            "VALUES (102, 'Law and administration');";
	private static final String FACULTY_INSERT_VAL4 = 
            "INSERT INTO Faculty "+
            "VALUES (103, 'Languages');";
	
	//INSERTING DATA TO CLASS
	private static final String CLASS_INSERT_VAL1 = 
			"INSERT INTO Class "+
            "VALUES (1000, 'Introduction to labour law', 102);";
	private static final String CLASS_INSERT_VAL2 = 
            "INSERT INTO Class "+
            "VALUES (1001, 'Graph algorithms', 100);";
	private static final String CLASS_INSERT_VAL3 = 
            "INSERT INTO Class "+
            "VALUES (1002, 'Existentialism in 20th century', 101);";
	private static final String CLASS_INSERT_VAL4 =
            "INSERT INTO Class "+
            "VALUES (1003, 'English grammar', 103);";
	private static final String CLASS_INSERT_VAL5 = 
            "INSERT INTO Class "+
            "VALUES (1004, 'From Plato to Kant', 101);";
	
	//INSERTING DATA TO ENROLLMENT
	private static final String ENROLLMENT_ISERT_VAL1 = 
            "INSERT INTO Enrollment "+
            "VALUES (1, 1000);";
	private static final String ENROLLMENT_INSERT_VAL2 =
            "INSERT INTO Enrollment "+
            "VALUES (1, 1002);";
	private static final String ENROLLMENT_INSERT_VAL3 =
            "INSERT INTO Enrollment "+
            "VALUES (1, 1003);";
	private static final String ENROLLMENT_INSERT_VAL4 = 
            "INSERT INTO Enrollment "+
            "VALUES (1, 1004);";
	private static final String ENROLLMENT_INSERT_VAL5 = 
            "INSERT INTO Enrollment "+
            "VALUES (2, 1002);";
	private static final String ENROLLMENT_INSERT_VAL6 = 
            "INSERT INTO Enrollment "+
            "VALUES (2, 1003);";
	private static final String ENROLLMENT_INSERT_VAL7 = 
            "INSERT INTO Enrollment "+
            "VALUES (4, 1000);";
	private static final String ENROLLMENT_INSERT_VAL8 = 
            "INSERT INTO Enrollment "+
            "VALUES (4, 1002);";
	private static final String ENROLLMENT_INSERT_VAL9 =
			"INSERT INTO Enrollment "+
            "VALUES (4, 1003);";
	
	//SELECTING QUERIES
	
	private static final String SELECT_QUERY1 = 
			 "SELECT pkey, name FROM Student";
	private static final String SELECT_QUERY2 = 
    		 "SELECT s.pkey, s.name "
			 + "FROM Student s LEFT JOIN Enrollment e ON (s.pkey=e.fkey_student) "
			 + "WHERE e.fkey_student IS NULL";
	private static final String SELECT_QUERY3 = 
			  "SELECT s.pkey, s.name "
	    	  + "FROM Student s LEFT JOIN Enrollment e ON (s.pkey=e.fkey_student) "
	    	  + "JOIN Class c ON (e.fkey_class=c.pkey)"
	    	  + "WHERE s.sex='female' AND c.name='Existentialism in 20th century'";
	private static final String SELECT_QUERY4 = 
    		"SELECT f.name "
	    	  + "FROM Student s FULL JOIN Enrollment e ON (s.pkey=e.fkey_student) "
	    	  + "FULL JOIN Class c ON (e.fkey_class=c.pkey)"
	    	  + " FULL JOIN Faculty f ON (c.fkey_faculty=f.pkey)"
	    	  + "WHERE c.pkey NOT IN (SELECT fkey_class FROM Enrollment)";
	private static final String SELECT_QUERY5 = 
    		  "SELECT MAX(s.age) maxAge "
	    	  + "FROM Student s FULL JOIN Enrollment e ON (s.pkey=e.fkey_student) "
	    	  + "FULL JOIN Class c ON (e.fkey_class=c.pkey)"
	    	  + "WHERE c.name='Introduction to labour law'";
	private static final String SELECT_QUERY6 = 
			  "SELECT c.name, COUNT(e.fkey_student) countFk "
	    	  + "FROM Enrollment e  "
	    	  + "FULL JOIN Class c ON (e.fkey_class=c.pkey)"
	    	  + "GROUP BY c.name "
	    	  + "HAVING COUNT(e.fkey_student)>=2  ";
	private static final String SELECT_QUERY7 =
			  "SELECT s.level, AVG(s.age) av "
			  + "FROM Student s "
		      + "GROUP BY s.level ";
	
	
	public static void main(String[] args) {
		
		BasicConfigurator.configure();
		
		try {
			con = DriverManager.getConnection(
			        "jdbc:hsqldb:hsql://127.0.0.1:9001/test-db",
			        "SA",
			        "");
			
			//STUDENTS
			
			statemet = con.prepareStatement(DROP_TABLE_STUDENT); 
			statemet.execute();
			statemet = con.prepareStatement(CREATE_TABLE_STUDENT);
			statemet.execute();
			statemet = con.prepareStatement(STUDENT_INSERT_VAL1);
			statemet.execute();
			statemet = con.prepareStatement(STUDENT_INSERT_VAL2
                    );
			statemet.execute();
			statemet = con.prepareStatement(STUDENT_INSERT_VAL3
                    );
			statemet.execute();
			statemet = con.prepareStatement(STUDENT_INSERT_VAL4);
			statemet.execute();
			
			//FACULTY
			  
			statemet = con.prepareStatement(DROP_TABLE_FACULTY); 
			statemet.execute();
			statemet = con.prepareStatement(CREATE_TABLE_FACULTY);
			statemet.execute();
			statemet = con.prepareStatement(FACULTY_INSERT_VAL1);
			statemet.execute();
			statemet = con.prepareStatement(FACULTY_INSERT_VAL2);
			statemet.execute();
			statemet = con.prepareStatement(FACULTY_INSERT_VAL3);
			statemet.execute();
			statemet = con.prepareStatement(FACULTY_INSERT_VAL4);
			statemet.execute();		
			
			//CLASS
	
			statemet = con.prepareStatement(DROP_TABLE_CLASS); 
			statemet.execute();		
			statemet = con.prepareStatement(CREATE_TABLE_CLASS);
			statemet.execute();
			statemet = con.prepareStatement(CLASS_INSERT_VAL1);
			statemet.execute();
			statemet = con.prepareStatement(CLASS_INSERT_VAL2);
			statemet.execute();
			statemet = con.prepareStatement(CLASS_INSERT_VAL3);
			statemet.execute();
			statemet = con.prepareStatement(CLASS_INSERT_VAL4);
			statemet.execute();
			statemet = con.prepareStatement(CLASS_INSERT_VAL5);
			statemet.execute();

			//ENROLLMENT
			
			statemet = con.prepareStatement(DROP_TABLE_ENROLLMENT); 
			statemet.execute();
			statemet = con.prepareStatement(CREATE_TABLE_ENROLLMENT);			
			statemet.execute();
			statemet = con.prepareStatement(ENROLLMENT_ISERT_VAL1);
			statemet.execute();
			statemet = con.prepareStatement(ENROLLMENT_INSERT_VAL2);
			statemet.execute();
			statemet = con.prepareStatement(ENROLLMENT_INSERT_VAL3);
			statemet.execute();
			statemet = con.prepareStatement(ENROLLMENT_INSERT_VAL4);
			statemet.execute();
			statemet = con.prepareStatement(ENROLLMENT_INSERT_VAL5);
			statemet.execute();
			statemet = con.prepareStatement(ENROLLMENT_INSERT_VAL6);
			statemet.execute();
			statemet = con.prepareStatement(ENROLLMENT_INSERT_VAL7);
			statemet.execute();
			statemet = con.prepareStatement(ENROLLMENT_INSERT_VAL8);
			statemet.execute();
			statemet = con.prepareStatement(ENROLLMENT_INSERT_VAL9);
			statemet.execute();	
			
			//SELECTING QUERIES 
			
				statemet = con.prepareStatement(SELECT_QUERY1);
			    ResultSet resultSet = statemet.executeQuery();
			    String result="\nFIRST_QUERY_SELECT_RESULT :\n";
			    while (resultSet.next()) {
			        int x = resultSet.getInt("pkey");
			        String s = resultSet.getString("name");
			        s = s.split(" ")[1];
			        
			        result += "pkey = "+x+ " "+"name = "+s+" "+"\n"; 
			    }
			    log.info(result);
			    
			    statemet = con.prepareStatement(SELECT_QUERY2);
			    resultSet = statemet.executeQuery();
			    result="\n\nSECOND_QUERY_RESULT:\n";
			    while (resultSet.next()) {
			        int x = resultSet.getInt("pkey");
			        String s = resultSet.getString("name");
			        s = s.split(" ")[1];
			        result += "pkey = "+x+ " "+"name = "+s+" "+"\n";			        
			    }			    
			    log.info(result);
			    
			    statemet = con.prepareStatement(SELECT_QUERY3);
			    resultSet = statemet.executeQuery();
			    result="\nTHIRD_QUERY_SELECT_RESULT :\n";
			    while (resultSet.next()) {
			        int x = resultSet.getInt("pkey");
			        String s = resultSet.getString("name");
			        s = s.split(" ")[1];
			        result += "pkey="+x+ " "+"name="+s+" "+"\n";
			    }
			    log.info(result);
			    
			    statemet = con.prepareStatement(SELECT_QUERY4);			    
			    resultSet = statemet.executeQuery();			    
			    result="\nFOURTH_QUERY_SELECT_RESULT :\n";
			    while (resultSet.next()) {			        
			        String s = resultSet.getString("name");            
			        result += "name="+s+" "+"\n";			        
			    }
			    log.info(result);
			    
			    statemet = con.prepareStatement(SELECT_QUERY5);			    
			    resultSet = statemet.executeQuery();			    
			    result="\nFIFTH_QUERY_SELECT_RESULT :\n";
			    while (resultSet.next()) {			        
			        int s = resultSet.getInt("maxAge");			        
			        result += "max_age="+s+" "+"\n";			        
			    }
			    log.info(result);

			    statemet = con.prepareStatement(SELECT_QUERY6);
			    resultSet = statemet.executeQuery();			    
			    result="\nSIXTH_QUERY_SELECT_RESULT :\n";
			    while (resultSet.next()) {			        
			        String s = resultSet.getString("name");			        
			        result += "name="+s+" "+"\n";			        
			    }
			    log.info(result);
			    
			    statemet = con.prepareStatement(SELECT_QUERY7);
			    resultSet = statemet.executeQuery();
			    result="\nSEVENTH_QUERY_SELECT_RESULT :\n";
			    while (resultSet.next()) {			        
			        int s = resultSet.getInt("level");
			        double a = resultSet.getDouble("av");
			        result += "level="+s+" "+"avg="+a+"\n";			        
			    }
			    log.info(result);
			    	
		} catch (SQLException e) {

			e.printStackTrace();
		}
		

	}

}
