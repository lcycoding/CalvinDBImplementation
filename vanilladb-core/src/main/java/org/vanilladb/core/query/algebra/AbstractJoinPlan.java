package org.vanilladb.core.query.algebra;

import java.util.HashSet;
import java.util.Set;

import org.vanilladb.core.storage.metadata.statistics.Histogram;

/**
 * An abstract {@link Plan} class corresponding to the <em>join</em> relational
 * algebra operator.
 */
public abstract class AbstractJoinPlan extends ReduceRecordsPlan {
	/**
	 * Returns a histogram that, for each field, approximates the value
	 * distribution of the join of the specified histograms.
	 * 
	 * <p>
	 * Assumes that:
	 * <ul>
	 * <li>Values in a bucket have the same frequency (uniform frequency)</li>
	 * <li>Given values within two equal ranges (of two joinable fields), all
	 * values in the range having smaller number of values appear in the range
	 * having larger number of values</li>
	 * <li>Distributions of values in different fields are independent with each
	 * other</li>
	 * </ul>
	 * </p>
	 * 
	 * @param hist1
	 *            the left-hand histogram
	 * @param hist2
	 *            the right-hand histogram
	 * @param fldName1
	 *            the left-hand join field
	 * @param fldName2
	 *            the right-hand join field
	 * @return a histogram that, for each field, approximates the value
	 *         distribution of the join
	 */
	public static Histogram joinHistogram(Histogram hist1, Histogram hist2,
			String fldName1, String fldName2) {
		Histogram prodHist = ProductPlan.productHistogram(hist1, hist2);
		Set<String> group = new HashSet<String>();
		group.add(fldName1);
		group.add(fldName2);
		return SelectPlan.joinFieldsHistogram(prodHist, group);
	}
}
