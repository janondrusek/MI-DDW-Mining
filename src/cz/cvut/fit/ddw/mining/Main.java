package cz.cvut.fit.ddw.mining;

import cz.cvut.fit.ddw.mining.dao.AllJoined;
import cz.cvut.fit.ddw.mining.dao.Apriori;
import cz.cvut.fit.ddw.mining.dao.Clustering;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

public class Main {

    public static void main(String[] args) {
        try {
            //insertApriori(getGroupedApriori(selectAllJoined()));
            insertClustering(getGroupedClustering(selectAllJoined()));
        } catch (Exception e) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public static List<Map<String, Object>> selectAllJoined() throws Exception {
        return AllJoined.getInstance().select();
    }

    public static List<Map<String, Object>> getGroupedApriori(List<Map<String, Object>> rows) {
        return getGrouped(rows, false);
    }

    public static List<Map<String, Object>> getGroupedClustering(List<Map<String, Object>> rows) {
        return getGrouped(rows, true);
    }

    public static List<Map<String, Object>> getGrouped(List<Map<String, Object>> rows, boolean clustering) {
        List<Map<String, Object>> grouped = new ArrayList<Map<String, Object>>();
        List<Map<String, Object>> visit = new ArrayList<Map<String, Object>>();
        Integer lastVisitId = 0;

        for (Map<String, Object> row : rows) {
            visit.add(row);

            if (!((Integer) row.get(AllJoined.VISIT_ID)).equals(lastVisitId) && !lastVisitId.equals(0)) {
                if (clustering) {
                    grouped.add(getClustering(visit));
                } else {
                    grouped.add(getApriori(visit));
                }
                visit = new ArrayList<Map<String, Object>>();
            }
            lastVisitId = (Integer) row.get(AllJoined.VISIT_ID);
        }
        return grouped;
    }

    public static Map<String, Object> getApriori(List<Map<String, Object>> visit) {
        Map<String, Object> apriori = new HashMap<String, Object>();

        apriori.put(Apriori.SOFT_CONVERSION, isSoftConversion(visit));
        apriori.put(Apriori.HARD_CONVERSION, isHardConversion(visit));
        apriori.put(Apriori.CATEGORY_NAMES, joinCategoryNames(visit));
        apriori.put(Apriori.TOPIC_NAMES, joinTopicNames(visit));
        apriori.put(Apriori.WEEKEND, isWeekend(visit));
        apriori.put(Apriori.TIME_OF_DAY, getTimeOfDay(visit));
        apriori.put(Apriori.REFERRER, getReferrer(visit));
        apriori.put(Apriori.LARGE_AMOUNT_OF_PAGES, isLargeAmountOfPages(visit));

        return apriori;
    }

    public static Map<String, Object> getClustering(List<Map<String, Object>> visit) {
        Map<String, Object> clustering = new HashMap<String, Object>();

        Set<String> topics = collectColumns(visit, AllJoined.TOPIC_NAME);
        Set<String> topics2 = new TreeSet<String>();

        for (String topic : topics) {
            topics2.add(topic.toLowerCase().replace(" ", ""));
        }

        for (String column : Clustering.getInstance().getCols()) {
            clustering.put(column, topics2.contains(column.toLowerCase().replace(" ", "")));
        }

        return clustering;
    }

    public static boolean isSoftConversion(List<Map<String, Object>> visit) {
        return isConversion(visit, AllJoined.SOFT_CONVERSION);
    }

    public static boolean isHardConversion(List<Map<String, Object>> visit) {
        return isConversion(visit, AllJoined.HARD_CONVERSION);
    }

    public static boolean isConversion(List<Map<String, Object>> visit, String[] array) {
        List<String> list = Arrays.asList(array);
        Set<String> collected = collectColumns(visit, AllJoined.PAGE_NAME);
        return CollectionUtils.containsAny(collected, list);
    }

    public static String joinCategoryNames(List<Map<String, Object>> visit) {
        return joinNames(visit, AllJoined.CAT_NAME);
    }

    public static String joinTopicNames(List<Map<String, Object>> visit) {
        return joinNames(visit, AllJoined.TOPIC_NAME);
    }

    public static String joinNames(List<Map<String, Object>> visit, String columnName) {
        return StringUtils.join(collectColumns(visit, columnName), "_");
    }

    public static boolean isWeekend(List<Map<String, Object>> visit) {
        Set<String> days = collectColumns(visit, AllJoined.DEN);
        return days.contains("Saturday") || days.contains("Sunday");
    }

    public static Set<String> collectColumns(List<Map<String, Object>> visit, String columnName) {
        Set<String> columns = new TreeSet<String>();
        for (Map<String, Object> page : visit) {
            columns.add((String) page.get(columnName));
        }
        return columns;
    }

    public static String getTimeOfDay(List<Map<String, Object>> visit) {
        String time = "Day";
        int hour = ((Integer) visit.get(0).get(AllJoined.HODINA)).intValue();
        if (hour >= 17 && hour <= 22) {
            time = "Evening";
        } else if (hour >= 23 && hour <= 11) {
            time = "Morning";
        }
        return time;
    }

    public static String getReferrer(List<Map<String, Object>> visit) {
        return (String) visit.get(0).get(AllJoined.TYP_ODKAZOVACE);
    }

    public static boolean isLargeAmountOfPages(List<Map<String, Object>> visit) {
        return visit.size() > 5;
    }

    public static void insertApriori(List<Map<String, Object>> apriori) throws Exception {
        Apriori.getInstance().insert(apriori);
    }

    public static void insertClustering(List<Map<String, Object>> clustering) throws Exception {
        Clustering.getInstance().insert(clustering);
    }
}
