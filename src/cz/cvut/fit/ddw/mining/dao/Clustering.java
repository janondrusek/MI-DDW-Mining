package cz.cvut.fit.ddw.mining.dao;

import java.util.List;
import java.util.Map;

public class Clustering extends QueryAbstract {

    public static final String ALPY = "Alpy";
    public static final String BULHARSKO = "Bulharsko";
    public static final String CERNA_HORA = "CernaHora";
    public static final String CYKLO = "Cyklo";
    public static final String EXOTIKA = "Exotika";
    public static final String EXPEDICE_NAROCNE = "ExpediceNarocne";
    public static final String GOLF = "Golf";
    public static final String HOROLEZECKA_SKOLA = "HorolezeckaSkola";
    public static final String HORSKA_TURISTIKA = "HorskaTuristika";
    public static final String HOTELBUSY = "Hotelbusy";
    public static final String KORSIKA = "Korsika";
    public static final String LASTMINUTE = "Lastminute";
    public static final String LIPARI = "Lipari";
    public static final String LYZE = "Lyze";
    public static final String NENI = "Neni";
    public static final String NEZJISTEN = "Nezjisten";
    public static final String OBECNE = "Obecne";
    public static final String POBYTOVE = "Pobytove";
    public static final String POBYTY_S_VYLETY = "PobytySVylety";
    public static final String POZNAVACI_ZAJEZDY_S_LEHKOU_TURISTIKOU = "PoznavaciZajezdySLehkouTuristikou";
    public static final String POZNAVACI_ZAJEZDY_S_UBYTOVANIM = "PoznavaciZajezdySUbytovanim";
    public static final String RAFTING = "Rafting";
    public static final String SKOLNI_ZAJEZDY = "SkolniZajezdy";
    public static final String TURISTIKA = "Turistika";
    public static final String UBYTOVANI_A_DOPRAVA = "UbytovaniADoprava";
    public static final String VHT = "VHT";
    public static final String VODA = "Voda";
    protected static Clustering _instance = null;

    private Clustering() {
    }

    public static Clustering getInstance() {
        if (_instance == null) {
            _instance = new Clustering();
        }
        return _instance;
    }

    public int insert(List<Map<String, Object>> clustering) throws Exception {
        int count = 0;
        try {
            initConnection();

            System.out.println(getQuery());
            preparedStatement = connect.prepareStatement(getQuery());
            for (Map<String, Object> row : clustering) {
                preparedStatement.setBoolean(1, (Boolean) row.get(ALPY));
                preparedStatement.setBoolean(2, (Boolean) row.get(BULHARSKO));
                preparedStatement.setBoolean(3, (Boolean) row.get(CERNA_HORA));
                preparedStatement.setBoolean(4, (Boolean) row.get(CYKLO));
                preparedStatement.setBoolean(5, (Boolean) row.get(EXOTIKA));
                preparedStatement.setBoolean(6, (Boolean) row.get(EXPEDICE_NAROCNE));
                preparedStatement.setBoolean(7, (Boolean) row.get(GOLF));
                preparedStatement.setBoolean(8, (Boolean) row.get(HOROLEZECKA_SKOLA));
                preparedStatement.setBoolean(9, (Boolean) row.get(HORSKA_TURISTIKA));
                preparedStatement.setBoolean(10, (Boolean) row.get(HOTELBUSY));
                preparedStatement.setBoolean(11, (Boolean) row.get(KORSIKA));
                preparedStatement.setBoolean(12, (Boolean) row.get(LASTMINUTE));
                preparedStatement.setBoolean(13, (Boolean) row.get(LIPARI));
                preparedStatement.setBoolean(14, (Boolean) row.get(LYZE));
                preparedStatement.setBoolean(15, (Boolean) row.get(NENI));
                preparedStatement.setBoolean(16, (Boolean) row.get(NEZJISTEN));
                preparedStatement.setBoolean(17, (Boolean) row.get(OBECNE));
                preparedStatement.setBoolean(18, (Boolean) row.get(POBYTOVE));
                preparedStatement.setBoolean(19, (Boolean) row.get(POBYTY_S_VYLETY));
                preparedStatement.setBoolean(20, (Boolean) row.get(POZNAVACI_ZAJEZDY_S_LEHKOU_TURISTIKOU));
                preparedStatement.setBoolean(21, (Boolean) row.get(POZNAVACI_ZAJEZDY_S_UBYTOVANIM));
                preparedStatement.setBoolean(22, (Boolean) row.get(RAFTING));
                preparedStatement.setBoolean(23, (Boolean) row.get(SKOLNI_ZAJEZDY));
                preparedStatement.setBoolean(24, (Boolean) row.get(TURISTIKA));
                preparedStatement.setBoolean(25, (Boolean) row.get(UBYTOVANI_A_DOPRAVA));
                preparedStatement.setBoolean(26, (Boolean) row.get(VHT));
                preparedStatement.setBoolean(27, (Boolean) row.get(VODA));

                count = preparedStatement.executeUpdate();
            }

        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }
        return count;
    }

    public String[] getCols() {
        return new String[]{
                    ALPY,
                    BULHARSKO,
                    CERNA_HORA,
                    CYKLO,
                    EXOTIKA,
                    EXPEDICE_NAROCNE,
                    GOLF,
                    HOROLEZECKA_SKOLA,
                    HORSKA_TURISTIKA,
                    HOTELBUSY,
                    KORSIKA,
                    LASTMINUTE,
                    LIPARI,
                    LYZE,
                    NENI,
                    NEZJISTEN,
                    OBECNE,
                    POBYTOVE,
                    POBYTY_S_VYLETY,
                    POZNAVACI_ZAJEZDY_S_LEHKOU_TURISTIKOU,
                    POZNAVACI_ZAJEZDY_S_UBYTOVANIM,
                    RAFTING,
                    SKOLNI_ZAJEZDY,
                    TURISTIKA,
                    UBYTOVANI_A_DOPRAVA,
                    VHT,
                    VODA
                };
    }

    @Override
    public String getQuery() {
        return "INSERT INTO Clustering (" + join(getCols(), ",") + ") VALUES (" + join(buildQuestionmarks(), ",") + ")";
    }
}
