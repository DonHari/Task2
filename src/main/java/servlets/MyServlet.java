package servlets;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import data.Contact;
import database.DataSource;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.beans.PropertyVetoException;
import java.io.*;
import java.sql.*;
import java.util.regex.Pattern;


@WebServlet("/hello/contacts")
public class MyServlet extends HttpServlet {

    private static final int FETCH_SIZE = 10000;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");

        //для отправки клиенту
        OutputStream out = resp.getOutputStream();
        PrintWriter printWriter = new PrintWriter(out);

        //получаем параметр url и обрабатываем его
        String paramValue = req.getParameter("namefilter");
        if(paramValue == null){
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);

            printWriter.write("Необходимо указать фильт \"namefilter\", в котором должно быть регулярное выражение");
        }
        else {
            Pattern pattern = Pattern.compile(paramValue);
            int code = sendDataAsJson(pattern, out);
            if(code == 1){
                resp.setStatus(HttpServletResponse.SC_NO_CONTENT);
                printWriter.write("В базе данных ничего не найдено.");
            }
            else{
                resp.setStatus(HttpServletResponse.SC_OK);
            }
        }
        out.close();

    }

    private int sendDataAsJson(Pattern pattern, OutputStream out){
        long count = 0;
        Connection con = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            con = DataSource.getInstance().getConnection();

            //проверка на наличие записей в БД
            statement = con.createStatement();
            rs = statement.executeQuery("select count(*) from contacts;");

            if(rs.next()){
                count = rs.getLong("count(*)");
            }

            statement.close();
            rs.close();

            //нет записей
            if(count == 0){
                con.close();
                return 1;//пусто
            }

            statement = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,//курсор двигается только в одну сторону
                    ResultSet.CONCUR_READ_ONLY//только читаем, больше никаких манипуляций над ними
            );
            statement.setFetchSize(FETCH_SIZE);

            rs = statement.executeQuery("select * from contacts;");

            String bufferName;
            //для читабельного вывода (если убрать, повысится скорость, но незначительно)
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            //для потокового вывода
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));
            writer.setIndent("  ");
            writer.beginArray();
            while(rs.next()){
                bufferName = rs.getString("name");
                if(!pattern.matcher(bufferName).matches()){
                    gson.toJson(new Contact(rs.getInt("id"), bufferName), Contact.class, writer);
                }
            }
            writer.endArray();
            writer.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (PropertyVetoException e) {
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
        }
        return 0;//okey
    }

}
