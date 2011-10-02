package cz.cvut.fit.ddw.mining.dao;

import java.util.List;
import java.util.Map;

public class Apriori extends QueryAbstract {

    public static final String SOFT_CONVERSION = "SoftConversion";
    public static final String HARD_CONVERSION = "HardConversion";
    public static final String CATEGORY_NAMES = "CategoryNames";
    public static final String TOPIC_NAMES = "TopicNames";
    public static final String WEEKEND = "Weekend";
    public static final String TIME_OF_DAY = "TimeOfDay";
    public static final String REFERRER = "Referrer";
    public static final String LARGE_AMOUNT_OF_PAGES = "LargeAmountOfPages";
    protected static Apriori _instance = null;

    private Apriori() {
    }

    public static Apriori getInstance() {
        if (_instance == null) {
            _instance = new Apriori();
        }
        return _instance;
    }

    public int insert(List<Map<String, Object>> apriori) throws Exception {
        int count = 0;
        try {
            initConnection();

            preparedStatement = connect.prepareStatement(getQuery());
            for (Map<String, Object> row : apriori) {
                preparedStatement.setBoolean(1, (Boolean) row.get(SOFT_CONVERSION));
                preparedStatement.setBoolean(2, (Boolean) row.get(HARD_CONVERSION));
                preparedStatement.setString(3, (String) row.get(CATEGORY_NAMES));
                preparedStatement.setString(4, (String) row.get(TOPIC_NAMES));
                preparedStatement.setBoolean(5, (Boolean) row.get(WEEKEND));
                preparedStatement.setString(6, (String) row.get(TIME_OF_DAY));
                preparedStatement.setString(7, (String) row.get(REFERRER));
                preparedStatement.setBoolean(8, (Boolean) row.get(LARGE_AMOUNT_OF_PAGES));

                count = preparedStatement.executeUpdate();
            }

        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }
        return count;
    }

    @Override
    public String[] getCols() {
        return new String[]{SOFT_CONVERSION, HARD_CONVERSION, CATEGORY_NAMES, TOPIC_NAMES, WEEKEND, TIME_OF_DAY, REFERRER, LARGE_AMOUNT_OF_PAGES};
    }

    @Override
    public String getQuery() {
        return "INSERT INTO Apriori (" + join(getCols(), ",") + ") VALUES (" + join(buildQuestionmarks(), ",") + ")";
    }
}
