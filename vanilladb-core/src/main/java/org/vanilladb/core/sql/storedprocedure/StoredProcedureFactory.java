package org.vanilladb.core.sql.storedprocedure;

public interface StoredProcedureFactory {
	
	StoredProcedure getStroredProcedure(int pid);
	
}
