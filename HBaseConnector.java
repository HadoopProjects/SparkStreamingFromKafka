import java.io.Serializable;
import java.sql.*;

/**
 * Created by rsahukar on 12/16/16.
 */
public class HBaseConnector implements scala.Serializable {

    private Connection connection = null;
    private Statement statement = null;
    private ResultSet rs = null;
    private PreparedStatement ps = null;
    private static int key = 0;

    public HBaseConnector(){
        try {
            init();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public void init() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection("jdbc:phoenix:localhost");
        statement = connection.createStatement();
    }

    public void insert(String string) throws SQLException {

        statement.executeUpdate("upsert into hbasetest values ("+key+++",'"+string+"')");
        connection.commit();
        System.out.println("Row inserted!");

    }



}
