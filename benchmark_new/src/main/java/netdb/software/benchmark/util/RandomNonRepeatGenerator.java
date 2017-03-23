package netdb.software.benchmark.util;

import java.util.Random;

public class RandomNonRepeatGenerator {
	private int[] array;
	private int curIndex = 0;
	private int size;
	private static Random rg = new Random(0);

	public RandomNonRepeatGenerator(int size) {
		array = new int[size];
		for (int i = 0; i < size; i++) {
			array[i] = i + 1;
		}
		this.size = size;
	}

	/**
	 * Not repeat, randomly choose a number from 1 to the initinalized size
	 */
	public int next() {
		int rndIndex = (int) ((array.length - 1 - curIndex) * rg.nextDouble())
				+ curIndex;
		int tmp = array[rndIndex];
		array[rndIndex] = array[curIndex];
		array[curIndex] = tmp;
		curIndex++;

		return tmp;
	}

	public void reset() {
		array = new int[size];
		for (int i = 0; i < size; i++) {
			array[i] = i + 1;
		}

		curIndex = 0;
	}
}
