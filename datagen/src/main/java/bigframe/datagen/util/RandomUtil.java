package bigframe.datagen.util;

import java.util.Random;

/**
 * Some common random seeds.
 * @author andy
 *
 */
public class RandomUtil {
	public static final int [] SEEDS_TABLE = {1234567, 365325451};
	
	public static int randInt(Random rand, int min, int max) {

    // nextInt is normally exclusive of the top value,
    // so add 1 to make it inclusive
	  
    int randomNum = rand.nextInt((max - min) + 1) + min;

    return randomNum;
	}
	
	public static long randLong(Random rand, long min, long max) {
	  
	  if (max == min) return max;
	  
	  long randomNum = rand.nextLong();
	  
	  // Only generate positive random number
	  randomNum = randomNum > 0 ? randomNum : -randomNum;
	  
    randomNum = randomNum % (max - min) + min;
    
    return randomNum;
	}
}
