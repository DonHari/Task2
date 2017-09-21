package database;

import org.apache.commons.dbcp.BasicDataSource;
import servlets.MyServlet;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;

public class DataSource {

    private static final long RECORD_COUNT = 1000000;//кол-во записей, которые будут в таблице (при измнении - удалить все из базы и перезапустить сервер)
    private static final long BATCH_SIZE = 10000;//сколько будет записываться за раз

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost/";
    private static final String DB_URL_PARAMS = "?useSSL=false";//знак вопроса должен быть
    private static final String DB_SCHEMA = "test";

    private static final String USER = "root";
    private static final String PASSWORD = "root";

    private static DataSource datasource;
    private BasicDataSource ds;

    private DataSource() throws IOException, SQLException, PropertyVetoException {
        ds = new BasicDataSource();
        ds.setDriverClassName(JDBC_DRIVER);
        ds.setUsername(USER);
        ds.setPassword(PASSWORD);
        ds.setUrl(DB_URL + DB_SCHEMA + DB_URL_PARAMS);

        ds.setMinIdle(0);//сколько минимум подлючений будет "сидеть без дела"

        //создать схему и таблицу
        createDatabaseAndTable();
        //заполняет таблицу данными: id генерируется внутри mysql, name будет формата "contact<счетчик_цикла>"
        fillTable();

        System.out.println("DataSource constructor");
    }

    public static DataSource getInstance() throws IOException, SQLException, PropertyVetoException {
        if (datasource == null) {
            datasource = new DataSource();
            return datasource;
        } else {
            return datasource;
        }
    }

    public Connection getConnection() throws SQLException {
        return this.ds.getConnection();
    }

    private static BufferedReader openConnectionToResFile(String path){
        InputStream is = DataSource.class.getResourceAsStream(path);
        return new BufferedReader(new InputStreamReader(is));
    }

    private static void createDatabaseAndTable(){
        Connection con = null;
        Statement statement = null;
        try {
            //подключение через чистый jdbc, потому что BasicDataSource не пропускает подключение без схемы
            Class.forName(JDBC_DRIVER);
            con = DriverManager.getConnection(DB_URL + DB_URL_PARAMS, USER, PASSWORD);

            //создание схему
            BufferedReader in = openConnectionToResFile("/sql/schema.sql");
            String buffer;
            StringBuilder sqlScript = new StringBuilder();
            while ((buffer = in.readLine()) != null){
                sqlScript.append(buffer);
            }
            statement = con.createStatement();
            statement.addBatch(sqlScript.toString());

            //создание таблицы
            in = openConnectionToResFile("/sql/table.sql");
            sqlScript = new StringBuilder();
            while ((buffer = in.readLine()) != null){
                sqlScript.append(buffer);
            }
            statement.addBatch(sqlScript.toString());

            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if(con != null){
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(statement != null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void fillTable(){

        Connection con = null;
        Statement statement = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            Class.forName(JDBC_DRIVER);
            con = DriverManager.getConnection(DB_URL + DB_SCHEMA + DB_URL_PARAMS, USER, PASSWORD);
            con.setAutoCommit(false);

            statement = con.createStatement();
            rs = statement.executeQuery("select count(*) from contacts;");
            if(rs.next()){
                //если таблица пустая
                if(rs.getInt("count(*)") == 0) {
                    ps = con.prepareStatement("insert into contacts(name) value(?);");

                    for (int i = 0; i < RECORD_COUNT; i++) {
                        ps.setString(1, "contact" + i);
                        ps.addBatch();
                        if ( BATCH_SIZE % (i + 1) == 0 ) {
                            ps.executeBatch();
                        }
                    }
                    ps.executeBatch();
                }
            }
            con.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if(con != null){
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(statement != null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
