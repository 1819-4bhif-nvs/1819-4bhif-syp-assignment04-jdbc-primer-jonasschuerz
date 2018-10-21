package at.htl.firedepartment;

import com.sun.org.glassfish.external.statistics.annotations.Reset;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.*;

public class FireDepartmentTest {

    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db;create=true";
    static final String USER="app";
    static final String PASSWORD="app";
    private static Connection conn;

    @BeforeClass
    public static void initJdbc(){

        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING,USER,PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Verbindung zur Datenbank nicht möglich:\n"+e.getMessage()+"\n");
            System.exit(1);
        }

        //Erstellung der Tables(Mitglied, Einsatz und Einsatzstatistik)
        try {
            Statement stmt = conn.createStatement();

            String sql = "CREATE TABLE mitglied(" +
                    "id INT CONSTRAINT mitglied_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
                    "name VARCHAR(200) NOT NULL," +
                    "dienstgrad varchar(10)," +
                    "email varchar(200))";
            stmt.execute(sql);

            String sqlf = "CREATE TABLE einsatz(" +
                    "id INT CONSTRAINT einsatz_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
                    "einsatzart VARCHAR(200) NOT NULL," +
                    "alarmtext VARCHAR (200)," +
                    "alarmstufe numeric," +
                    "ort VARCHAR(200)," +
                    "zeit varchar(200))";
            stmt.execute(sqlf);

            String sqlS = "CREATE TABLE einsatzstatistik(" +
                    "id INT CONSTRAINT einsatzstatistik_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
                    "mitglied_id int constraint mitglied_fk references mitglied," +
                    "einsatz_id int constraint einsatz_fk references einsatz," +
                    "information varchar(200))";
            stmt.execute(sqlS);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dml(){

        //Beffüllen der Tables mit Daten
        int countInserts = 0;
        try {
            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO mitglied (name,dienstgrad,email) VALUES ('Jonas Schürz','FM','jonas.test@gmail.com')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO mitglied (name,dienstgrad,email) VALUES ('Gebhard Gangl','HBI','gebhard.test@gmail.com')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO mitglied (name,dienstgrad,email) VALUES ('Franz Mayr','OFM','franz.test@gmail.com')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO EINSATZ (einsatzart,alarmtext,alarmstufe,ort,zeit) VALUES ('Fahrzeugbergung','Traktor im Teich',1,'Riedl Abzw. Kammerschlag','28.08.2018')";
            countInserts += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat(countInserts,is(4));

        try {
            PreparedStatement pstmt = conn.prepareStatement("Select id,name,dienstgrad,email from mitglied");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("name"),is("Jonas Schürz"));
            assertThat(rs.getString("dienstgrad"),is("FM"));
            assertThat(rs.getString("email"),is("jonas.test@gmail.com"));
            rs.next();
            assertThat(rs.getString("name"),is("Gebhard Gangl"));
            assertThat(rs.getString("dienstgrad"),is("HBI"));
            assertThat(rs.getString("email"),is("gebhard.test@gmail.com"));
            rs.next();
            assertThat(rs.getString("name"),is("Franz Mayr"));
            assertThat(rs.getString("dienstgrad"),is("OFM"));
            assertThat(rs.getString("email"),is("franz.test@gmail.com"));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            PreparedStatement pstmt = conn.prepareStatement("Select id,einsatzart,alarmtext,alarmstufe,ort,zeit from EINSATZ");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("einsatzart"),is("Fahrzeugbergung"));
            assertThat(rs.getString("alarmtext"),is("Traktor im Teich"));
            assertThat(rs.getInt("alarmstufe"),is(1));
            assertThat(rs.getString("ort"),is("Riedl Abzw. Kammerschlag"));
            assertThat(rs.getString("zeit"),is("28.08.2018"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void metadata(){
        Statement stmt = null;

        //Prüfen der Metadaten von Table Mitglied
        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * from MITGLIED");
            ResultSetMetaData rsmd = rs.getMetaData();

            assertThat(rsmd.getColumnCount(),is(4));
            assertThat(rsmd.getColumnName(1),is("ID"));
            assertThat(rsmd.getColumnName(2),is("NAME"));
            assertThat(rsmd.getColumnName(3),is("DIENSTGRAD"));
            assertThat(rsmd.getColumnName(4),is("EMAIL"));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Prüfen der Metadaten von Table Einsatz
        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * from EINSATZ");
            ResultSetMetaData rsmd = rs.getMetaData();

            assertThat(rsmd.getColumnCount(),is(6));
            assertThat(rsmd.getColumnName(1),is("ID"));
            assertThat(rsmd.getColumnName(2),is("EINSATZART"));
            assertThat(rsmd.getColumnName(3),is("ALARMTEXT"));
            assertThat(rsmd.getColumnName(4),is("ALARMSTUFE"));
            assertThat(rsmd.getColumnName(5),is("ORT"));
            assertThat(rsmd.getColumnName(6),is("ZEIT"));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Prüfen der Metadaten von Table Einsatzstatistik
        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * from EINSATZSTATISTIK");
            ResultSetMetaData rsmd = rs.getMetaData();

            assertThat(rsmd.getColumnCount(),is(4));
            assertThat(rsmd.getColumnName(1),is("ID"));
            assertThat(rsmd.getColumnName(2),is("MITGLIED_ID"));
            assertThat(rsmd.getColumnName(3),is("EINSATZ_ID"));
            assertThat(rsmd.getColumnName(4),is("INFORMATION"));
        } catch (SQLException e) {
            e.printStackTrace();
        }



    }

    @AfterClass
    public static void teardownJdbc(){
        try{
            conn.createStatement().execute("DROP TABLE einsatzstatistik");
            System.out.println("Tabelle Einsatzstatistik gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle Einsatzstatistik konnte nicht gelöscht werden:\n"+e.getMessage()+"\n");
        }
        try{
            conn.createStatement().execute("DROP TABLE mitglied");
            System.out.println("Tabelle Mitglied gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle Mitglied konnte nicht gelöscht werden:\n"+e.getMessage()+"\n");
        }
        try{
            conn.createStatement().execute("DROP TABLE einsatz");
            System.out.println("Tabelle Einsatz gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle Einsatz konnte nicht gelöscht werden:\n"+e.getMessage()+"\n");
        }

        try {
            if (conn !=null || !conn.isClosed()){
                conn.close();
                System.out.println("Goodbye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
