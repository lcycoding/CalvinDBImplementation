package org.vanilladb.core.storage.tx.recovery;

import java.util.Map;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

class IndexRecoveryUtil {

	protected static void insertIntoIndex(String dataTblName, String fldName,
			Constant keyVal, RecordId rid, Transaction tx, long txNum) {
		Map<String, IndexInfo> iiMap = VanillaDb.catalogMgr().getIndexInfo(
				dataTblName, tx);
		IndexInfo ii = iiMap.get(fldName);

		if (ii != null) {
			Index idx = ii.open(tx);
			idx.beforeFirst(ConstantRange.newInstance(keyVal));
			boolean existed = false;
			while (idx.next()) {
				if (idx.getDataRecordId().equals(rid)) {
					existed = true;
					break;
				}
			}
			// avoid duplicated insertion
			if (!existed)
				idx.insert(keyVal, rid);
			idx.close();
		}
	}

	protected static void deleteFromIndex(String dataTblName, String fldName,
			Constant keyVal, RecordId rid, Transaction tx, long txNum) {
		Map<String, IndexInfo> iiMap = VanillaDb.catalogMgr().getIndexInfo(
				dataTblName, tx);
		IndexInfo ii = iiMap.get(fldName);

		if (ii != null) {
			Index idx = ii.open(tx);
			idx.delete(keyVal, rid);
			idx.close();
		}
	}
}