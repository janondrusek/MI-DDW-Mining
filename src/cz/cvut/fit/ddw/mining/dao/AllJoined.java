package cz.cvut.fit.ddw.mining.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AllJoined extends QueryAbstract {

    public static final String VISIT_ID = "VisitID";
    public static final String PAGE_NAME = "PageName";
    public static final String CAT_NAME = "CatName";
    public static final String TOPIC_NAME = "TopicName";
    public static final String DEN = "Den";
    public static final String HODINA = "Hodina";
    public static final String TYP_ODKAZOVACE = "TypOdkazovace";
    public static final String[] COLS = {VISIT_ID, PAGE_NAME, CAT_NAME, TOPIC_NAME, DEN, HODINA, TYP_ODKAZOVACE};
    public static final String[] HARD_CONVERSION = new String[]{"n_sleva.asp", "jak_se_prihlasit.htm", "n_pojistenick.asp", "n_kdojsme.asp"};
    public static final String[] SOFT_CONVERSION = new String[]{"n_prihlaska.asp", "n_katalog.asp"};
    public static final String[] TOPIC_NAME_NOT_IN = new String[]{"Neni", "Obecne", "Nezjisten"};
    protected static AllJoined _instance = null;

    private AllJoined() {
    }

    public static AllJoined getInstance() {
        if (_instance == null) {
            _instance = new AllJoined();
        }
        return _instance;
    }

    public List<Map<String, Object>> select() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        try {
            initConnection();
            // Statements allow to issue SQL queries to the database
            statement = connect.createStatement();
            // Result set get the result of the SQL query
            resultSet = statement.executeQuery(getQuery());
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<String, Object>();

                row.put(VISIT_ID, resultSet.getInt(VISIT_ID));
                row.put(PAGE_NAME, resultSet.getString(PAGE_NAME));
                row.put(CAT_NAME, resultSet.getString(CAT_NAME));
                row.put(TOPIC_NAME, resultSet.getString(TOPIC_NAME));
                row.put(DEN, resultSet.getString(DEN));
                row.put(HODINA, resultSet.getInt(HODINA));
                row.put(TYP_ODKAZOVACE, resultSet.getString(TYP_ODKAZOVACE));

                rows.add(row);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }
        return rows;
    }

    @Override
    public String getQuery() {
        return "SELECT " + join(COLS, ",") + " " + "FROM all_joined ";
    }
}
