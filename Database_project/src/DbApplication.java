import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Scanner;
import java.util.Vector;
import java.util.stream.Stream;


import java.sql.JDBCType;


public class DbApplication {
	

	private Connection conn;
	

	public DbApplication() {
		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("Driver Found!");
		} catch (ClassNotFoundException e) {
			System.out.println("Driver not found!");
		}

	}
	
	public void dBappConnect(String ip, String dbappName, String username, String password) {
			try {
				conn = DriverManager.getConnection("jdbc:postgresql://" + ip + "/" + dbappName, username, password);
				System.out.println("The connection was established!");
				conn.setAutoCommit(false);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	public void dBappClose() {
		try {
			conn.close();
			System.out.println("Connection closed!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void commit() {
		try {
			conn.commit();
			System.out.println("Commit made!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void abort() {
		try {
			conn.rollback();
			System.out.println("Abort!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	
	public Vector<String> displayGradesOfStudent(int academic_year, // Semester_season_type 
			String academic_season, int amka) {
		try {
			PreparedStatement stmt = conn.prepareStatement("select * from(\r\n" + 
					"select nextval('ergasia') as id_num,course_code,course_title,lab_grade,final_grade \r\n" + 
					"from (\"Register\" natural join \"Course\" natural join \"CourseRun\") inner join \"Semester\" on \r\n" + 
					"(\"Semester\".semester_id = \"CourseRun\".semesterrunsin)\r\n" + 
					"where amka = 15 and academic_year=2018 and academic_season = 'winter' and (register_status = 'pass' or register_status = 'fail')\r\n" + 
					"order by course_code) as table_1_5");

			        ResultSet result = stmt.executeQuery(); 
			        
			       
			        Vector<String> results =  new Vector<String>();
			        
			while (result.next()) {
				System.out.println("id= "+result.getLong(1)+ " course code= "+result.getString(2)+" course title= "+ result.getString(3) + "\nlab grade= " + result.getDouble(4)+
				" final grade= " +result.getDouble(5));
				results.add(String.valueOf(result.getLong(1)));
				results.add(result.getString(2));
				
			}
			
			result.close();
			return results;
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return null;
	}


	public void displayStudentsOfACourse(String course_code, // Semester_season_type 
			String academic_season ,int academic_year) {
		try {
			PreparedStatement stmt = conn.prepareStatement("select amka, name, father_name, surname, email, am, entry_date  \r\n" + 
					"from ((\"Student\" st natural join \"Register\" r)\r\n" + 
					"natural join \"CourseRun\" cr)\r\n" + 
					"inner join \"Semester\" sem \r\n" + 
					"on (cr.semesterrunsin = sem.semester_id)\r\n" + 
					"where (register_status = 'approved' and academic_year = "+academic_year+"  and academic_season = '"+academic_season+"'"
				    + " and course_code = '"+course_code+"' )");
			
			        ResultSet result = stmt.executeQuery();
			        
			        
			        
		
			while (result.next()) {
				System.out.println("\namka= "+result.getInt(1)+ " name= "+result.getString(2)+" father_name= "+ result.getString(3) + "surname= " + result.getString(4) +
						"email= " +result.getString(5) +  " am= "+  result.getString(6)+  " entry_date= "+result.getDate(7));
			}
			
			result.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	
	public void update_grades_of_Student(double lab_grade, double exam_grade, int id,int amka,Vector<String> table) {
		
		
		
		try {
			
			int sizeOfTable = table.size();
			
			if (id-1 > sizeOfTable) {
				System.out.println("INDE OUT OF BOUNDS");
				return;
			}
			
			String course_code = table.elementAt((id - 1)*2 + 1);
			PreparedStatement stmt = conn.prepareStatement("update \"Register\"  \r\n" + 
					"set lab_grade = "+lab_grade+", exam_grade = "+exam_grade+", final_grade = 0.5*"+exam_grade+" + 0.5*"+lab_grade+"\r\n" + 
					"where course_code = '"+course_code+"' and amka = "+amka+";");
			
			stmt.executeUpdate(); 
			stmt.close();			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		
	}
	
		
	public static void main(String[] args) {
		
		DbApplication dbapp = new DbApplication();
		Scanner reader = new Scanner(System.in);
		int action = 0;
		do {
		System.out.println("1. Establish connection");
		System.out.println("2. Commit current transaction or begin a new one");
		System.out.println("3. Cancel current transaction or begin a new one");
		System.out.println("4. Display students that are enrolled in a specific course");
		System.out.println("5. Display students grades for a given semester");
		System.out.println("6. Cancel connection");
		System.out.println("Enter your action: ");
		
		
		
        action = reader.nextInt();
        reader.nextLine();
	
        Vector<String> grades_info = new Vector<String>();
        int academic_year = 0;
        String academic_season = "";
        int amka = 0;
        String course_code = "";
        int id = -1;
		double lab_grade = 0;
		double exam_grade = 0;
        
		switch (action) {
		case 1:
			System.out.println("Enter IP:");
			String ip = reader.nextLine();
			System.out.println("Enter the name of the database:");
			String dbappName=reader.nextLine();
			System.out.println("Enter username:");
			String username=reader.nextLine();
			System.out.println("Enter password:");
			String password = reader.nextLine();
			dbapp.dBappConnect(ip, dbappName, username, password);
			break;
		case 2:
			dbapp.commit();
			break;
		case 3:
			dbapp.abort();
			break;
		case 4:
			System.out.println("Enter the academic year:");
			academic_year = reader.nextInt();
			reader.nextLine();
			System.out.println("Enter the academic season:"); 
			academic_season = reader.nextLine();
			System.out.println("Enter the course code:");
			course_code=reader.nextLine();
			dbapp.displayStudentsOfACourse(course_code, academic_season, academic_year);
			break;	
		case 5:
						
			System.out.println("Enter the academic year:");
			academic_year = reader.nextInt();
			reader.nextLine();
			System.out.println("Enter the academic semester:"); 
		    academic_season = reader.nextLine();
		    System.out.println("Enter the amka of the student you want:");
			amka =reader.nextInt();
			reader.nextLine();
			grades_info = dbapp.displayGradesOfStudent(academic_year,academic_season,amka);
			do {
			System.out.println("Enter id:");
			id = reader.nextInt();
		    System.out.println("Enter new lab grade:");
            lab_grade = reader.nextDouble();
            System.out.println("Enter new exam grade:");
            exam_grade = reader.nextDouble();

            
		    if (id == 0) break;
		    if (id == -1) {
		    	System.out.println("ERROR");
		    	//THIS IS YET TO BE IMPLEMENTED

		    }
		    
		    dbapp.update_grades_of_Student(lab_grade, exam_grade, id, amka, grades_info);
		    
			} while (id != 0 && id!= -1);
		case 6:
			dbapp.dBappClose();
			break;
		}
		} while (action != 6);
		reader.close();
	}


}
