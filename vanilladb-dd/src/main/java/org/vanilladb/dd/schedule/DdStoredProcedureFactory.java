package org.vanilladb.dd.schedule;


public interface DdStoredProcedureFactory {

	DdStoredProcedure getStoredProcedure(int pid, long txNum);

}
