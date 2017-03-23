package netdb.software.benchmark.procedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class MicroBenchmarkProcParamHelper extends StoredProcedureParamHelper {

	private int readCount;
	private int writeCount;
	private int[] readItemId;
	private int[] writeItemId;
	private double[] newItemPrice;

	public int getReadCount() {
		return readCount;
	}

	public int getWriteCount() {
		return writeCount;
	}

	public int[] getReadItemId() {
		return readItemId;
	}

	public int[] getWriteItemId() {
		return writeItemId;
	}

	public double[] getNewItemPrice() {
		return newItemPrice;
	}

	@Override
	public void prepareParameters(Object... pars) {
		int indexCnt = 0;

		readCount = (Integer) pars[indexCnt++];
		readItemId = new int[readCount];
		for (int i = 0; i < readCount; i++)
			readItemId[i] = (Integer) pars[indexCnt++];

		writeCount = (Integer) pars[indexCnt++];
		writeItemId = new int[writeCount];
		for (int i = 0; i < writeCount; i++)
			writeItemId[i] = (Integer) pars[indexCnt++];
		newItemPrice = new double[writeCount];
		for (int i = 0; i < writeCount; i++)
			newItemPrice[i] = (Double) pars[indexCnt++];

		if (writeCount == 0)
			setReadOnly(true);
	}

	@Override
	public SpResultSet createResultSet() {
		/*
		 * TODO The output information is not strictly followed the TPC-C
		 * definition. See the session 2.4.3.5 in TPC-C 5.11 document.
		 */
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		sch.addField("status", statusType);

		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));

		return new SpResultSet(sch, rec);
	}

}
