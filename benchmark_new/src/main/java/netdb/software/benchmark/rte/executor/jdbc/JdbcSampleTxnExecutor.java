package netdb.software.benchmark.rte.executor.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import netdb.software.benchmark.TransactionType;
import netdb.software.benchmark.rte.txparamgen.MicrobenchmarkParamGen;
import netdb.software.benchmark.rte.txparamgen.TxParamGenerator;
import netdb.software.benchmark.util.JdbcService;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;

public class JdbcSampleTxnExecutor extends JdbcTxnExecutor {

	private String itemName;
	private int itemId;
	private boolean isCommitted;
	Connection conn;

	public JdbcSampleTxnExecutor() {}

	@Override
	protected void prepareParams() {
		// prepare your parameter here
		TxParamGenerator pg = new MicrobenchmarkParamGen();
		itemId = (Integer) pg.generateParameter()[0];
	}

	public void executeSql() {

		conn = JdbcService.connect();
		try {

			conn.setAutoCommit(false);
			
			Statement stm = JdbcService.createStatement(conn);
			ResultSet rs = null;

			String sql = "SELECT i_name FROM item WHERE i_id = " + itemId;
			rs = JdbcService.executeQuery(sql, stm);
			rs.beforeFirst();
			if (rs.next()) {
				itemName = rs.getString("i_name");
			} else
				throw new RuntimeException();
			rs.close();
			
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			JdbcService.disconnect(conn);
		}
	}

	protected SpResultSet createResultSet() {
		/*
		 * TODO The output information is not strictly followed the TPC-C
		 * definition. See the session 2.6.3.4 in TPC-C 5.11 document.
		 */
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		Type var16 = Type.VARCHAR(16);
		sch.addField("status", statusType);
		sch.addField("item_name", var16);

		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));
		rec.setVal("item_name", new VarcharConstant(itemName, var16));

		return new SpResultSet(sch, rec);
	}

	@Override
	protected TransactionType getTrasactionType() {
		return TransactionType.MICROBENCHMARK_TXN;
	}

}
