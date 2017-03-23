package org.vanilladb.core.sql.storedprocedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;

/**
 * An abstract class that denotes the stored procedure supported in VanillaDb.
 */
public interface StoredProcedure {

	void prepare(Object... pars);

	SpResultSet execute();

}
