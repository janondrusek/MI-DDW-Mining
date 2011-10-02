package cz.cvut.fit.ddw.mining.dao;

import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;

abstract public class QueryAbstract {

    protected Connection connect = null;
    protected Statement statement = null;
    protected PreparedStatement preparedStatement = null;
    protected ResultSet resultSet = null;

    protected static String quote(String[] array) {
        return "\"" + join(array) + "\"";
    }

    protected static String join(String[] array) {
        return join(array, "\",\"");
    }

    protected static String join(String[] array, String glue) {
        return StringUtils.join(array, glue);
    }

    protected String[] buildQuestionmarks() {
        String[] questionmarks = new String[getCols().length];
        Arrays.fill(questionmarks, "?");
        return questionmarks;
    }

    protected void initConnection() throws Exception {
        // This will load the MySQL driver, each DB has its own driver
        Class.forName("com.mysql.jdbc.Driver");
        // Setup the connection with the DB
        connect = DriverManager.getConnection("jdbc:mysql://localhost/ddw_6", "root", "");
    }

    protected void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {
            Logger.getLogger(AllJoined.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    abstract public String getQuery();

    public String[] getCols() {
        return new String[0];
    }
}
