package wdsr.exercise5.dao;

import java.sql.Types;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import wdsr.exercise5.model.Trade;

@Repository
public class TradeDao {

	@Autowired
	JdbcTemplate jdbcTemplate;

	private static final Logger LOGGER = LogManager.getLogger(TradeDao.class);
	
	/**
	 * Zaimplementuj metode insertTrade aby wstawiała nowy rekord do tabeli
	 * "trade" na podstawie przekazanego objektu klasy Trade.
	 * 
	 * @param trade
	 * @return metoda powinna zwracać id nowego rekordu.
	 */
	public int insertTrade(Trade trade) {

		String insertSql = "INSERT INTO Trade (asset, amount, date) VALUES (?, ?, ?)";
        Object[] params = new Object[] { trade.getAsset(), trade.getAmount(), trade.getDate() };
        int[] types = new int[] { Types.VARCHAR, Types.DOUBLE, Types.DATE };
        int row = jdbcTemplate.update(insertSql, params, types);
        LOGGER.info(row + " row inserted.");

        
        String select = "SELECT * FROM Trade ";
    	List<Trade> trades  = jdbcTemplate.query(select,new BeanPropertyRowMapper(Trade.class));    
    	int id = trades.get(trades.size()-1).getId();
    	
		return id;
	}

	/**
	 * Zaimplementuj metode aby wyciągneła z bazy rekord o podanym id. Użyj
	 * intrfejsu RowMapper.
	 * 
	 * @param id
	 * @return metaoda powinna zwracać obiekt reprezentujący rekord o podanym
	 *         id.
	 */
	public Optional<Trade> extractTrade(int id) {
		String select = "SELECT * FROM Trade where id="+id;
    	List<Trade> trades  = jdbcTemplate.query(select,new BeanPropertyRowMapper(Trade.class));    
    	
    	if(trades.isEmpty()){
    		return Optional.empty();
    	}
    	return Optional.of(trades.get(0));
		
	}

	/**
	 * Zaimplementuj metode aby wyciągneła z bazy rekord o podanym id.
	 * 
	 * @param id,
	 *            rch - callback który przetworzy wyciągnięty wiersz.
	 * @return metaoda powinna zwracać obiekt reprezentujący rekord o podanym
	 *         id.
	 */
	public void extractTrade(int id, RowCallbackHandler rch) {
		String select = "SELECT * FROM Trade where id="+id;
    	jdbcTemplate.query(select,rch);    
    
	}

	/**
	 * Zaimplementuj metode aby zaktualizowała rekord o podanym id danymi z
	 * przekazanego parametru 'trade'
	 * 
	 * @param trade
	 */
	public void updateTrade(int id, Trade trade) {
		String updateSql = "update Trade set asset = ?, amount = ?,date = ? where id = ?";
        Object[] params = new Object[] { trade.getAsset(), trade.getAmount(), trade.getDate(),id };
        int[] types = new int[] { Types.VARCHAR, Types.DOUBLE, Types.DATE,Types.INTEGER  };
        int row = jdbcTemplate.update(updateSql, params, types);
        LOGGER.info(row + " row updated.");

	}

	/**
	 * Zaimplementuj metode aby usuwała z bazy rekord o podanym id.
	 * 
	 * @param id
	 */
	public void deleteTrade(int id) {
		String deleteSql = "delete from Trade where id = ?";
        int row = jdbcTemplate.update(deleteSql, id);
        LOGGER.info(row + " row deleted.");

	}

}
