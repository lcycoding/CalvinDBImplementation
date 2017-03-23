package org.vanilladb.dd.schedule.naive;

import org.vanilladb.dd.schedule.DdStoredProcedureFactory;

public interface NaiveStoredProcedureFactory extends DdStoredProcedureFactory {
	
	@Override
	NaiveStoredProcedure<?> getStoredProcedure(int pid, long txNum);
	
}
