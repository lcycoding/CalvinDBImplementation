package org.vanilladb.dd.schedule.calvin;

import org.vanilladb.dd.schedule.DdStoredProcedureFactory;

public interface CalvinStoredProcedureFactory extends DdStoredProcedureFactory {

	@Override
	CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum);
}
