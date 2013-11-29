package bigframe.datagen.relational.tpcds;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import bigframe.bigif.BigConfConstants;
import bigframe.datagen.graph.kroneckerGraph.KroneckerGraphGen;
import bigframe.datagen.nested.tweet.RawTweetGenConstants;
import bigframe.util.Config;
import bigframe.datagen.util.RandomSeeds;
import cern.jet.random.engine.MersenneTwister;
import cern.jet.random.engine.RandomEngine;
import cern.jet.random.sampling.RandomSampler;

public class CollectTPCDSstatNaive extends CollectTPCDSstat {
	private static final Logger LOG = Logger
			.getLogger(CollectTPCDSstatNaive.class);
	/**
	 * The valid scale factor used in TPCDS.
	 */
	private final int[] arScaleVolume = { 1, 10, 100, 300, 1000, 3000, 10000,
			30000, 100000 };

	/**
	 * Use to calculate the cardinality of TPCDS table given a targetGB. It is a
	 * weight_set collected from the TPCDS generator.
	 */
	private final int[][] TPCDS_ROWCOUNT_WEIGHTSET = {
			{ 3, 11721, 11720, 11736, 11836, 11886, 31094, 104143, 111343,
				111363, 111362, 111371, 111671, 111706, 111726, 111732,
				111731, 111755, 198155, 198160, 198190, 198189, 198249,
				198264, 198265, 198264, 198289, 198290, 198291, 198290,
				198289, 198439, 198438, 198437, 198436, 198435, 198434,
				198439, 198438, 198439, 198444, 198443, 198442, 198441,
				198440, 198445, 198453, 198452, 198451, 198452, 198451,
				198450, 198449, 198448, 198449, 198453, 198452, 198458,
				198457, 198456, 198457, 198557, 199557, 199657, 199667,
				199669, 199671, 199673, 199675, 199676, 199677 },
			{ 12, 12012, 12011, 12171, 12671, 12921, 32129, 105178, 112378,
				112398, 112397, 112448, 112948, 112993, 113013, 113064,
				113063, 113303, 199703, 199713, 199813, 199812, 200412,
				200433, 200434, 200433, 200473, 200474, 200484, 200483,
				200482, 200692, 200691, 200690, 200689, 200688, 200687,
				200697, 200696, 200706, 200713, 200712, 200711, 200710,
				200709, 200716, 200724, 200723, 200722, 200723, 200722,
				200721, 200720, 200719, 200720, 200740, 200739, 200740,
				200739, 200738, 200739, 200839, 201839, 201939, 201949,
				201952, 201955, 201958, 201964, 201967, 201970 },
			{ 15, 20415, 20414, 22014, 24014, 25014, 44222, 117271, 124471,
				124491, 124490, 124592, 125592, 125647, 125667, 125868,
				125867, 128267, 214667, 214682, 215702, 215701, 221701,
				221713, 221714, 221713, 221813, 221814, 221914, 221913,
				221912, 222152, 222151, 222150, 222149, 222148, 222147,
				222167, 222166, 222266, 222276, 222275, 222274, 222273,
				222272, 222282, 222362, 222361, 222360, 222362, 222361,
				222360, 222359, 222358, 222359, 222399, 222398, 222598,
				222597, 222596, 222597, 222697, 223697, 223797, 223807,
				223811, 223815, 223819, 223837, 223846, 223855 },
			{ 18, 26018, 26017, 30817, 35817, 38317, 57525, 130574, 137774,
				137794, 137793, 137925, 139225, 139285, 139305, 139707,
				139706, 146906, 233306, 233323, 234625, 234624, 252624,
				252645, 252646, 252645, 252895, 252896, 253196, 253195,
				253194, 253434, 253433, 253432, 253431, 253430, 253429,
				253479, 253478, 253778, 253791, 253790, 253789, 253788,
				253787, 253800, 254040, 254039, 254038, 254042, 254041,
				254040, 254039, 254038, 254039, 254159, 254158, 254418,
				254417, 254416, 254417, 254517, 255517, 255617, 255627,
				255632, 255637, 255642, 255672, 255687, 255700 },
			{ 21, 30021, 30020, 46020, 58020, 64020, 83228, 156277, 163477,
				163497, 163496, 163646, 165146, 165211, 165231, 165732,
				165731, 189731, 276131, 276151, 277651, 277650, 337650,
				337677, 337678, 337677, 338277, 338278, 339278, 339277,
				339276, 339516, 339515, 339514, 339513, 339512, 339511,
				339631, 339630, 340630, 340645, 340644, 340643, 340642,
				340641, 340656, 341456, 341455, 341454, 341459, 341458,
				341457, 341456, 341455, 341456, 341856, 341855, 342155,
				342154, 342153, 342154, 342254, 343254, 343354, 343364,
				343369, 343374, 343379, 343433, 343460, 343481 },
			{ 24, 36024, 36023, 84023, 114023, 129023, 148231, 221280, 228480,
				228500, 228499, 228679, 230479, 230546, 230566, 231241,
				231240, 303240, 389640, 389662, 391462, 391461, 571461,
				571494, 571495, 571494, 572994, 572995, 575995, 575994,
				575993, 576233, 576232, 576231, 576230, 576229, 576228,
				576528, 576527, 579527, 579545, 579544, 579543, 579542,
				579541, 579559, 581959, 581958, 581957, 581963, 581962,
				581961, 581960, 581959, 581960, 583160, 583159, 583519,
				583518, 583517, 583518, 583618, 584618, 584718, 584728,
				584733, 584738, 584743, 584833, 584878, 584907 },
			{ 27, 40027, 40026, 200026, 265026, 297526, 316734, 389783, 396983,
				397003, 397002, 397203, 399203, 399273, 399293, 400043,
				400042, 640042, 726442, 726467, 728468, 728467, 1328467,
				1328506, 1328507, 1328506, 1331756, 1331757, 1341757,
				1341756, 1341755, 1341995, 1341994, 1341993, 1341992,
				1341991, 1341990, 1342640, 1342639, 1352639, 1352659,
				1352658, 1352657, 1352656, 1352655, 1352675, 1360675,
				1360674, 1360673, 1360680, 1360679, 1360678, 1360677,
				1360676, 1360677, 1364677, 1364676, 1365076, 1365075,
				1365074, 1365075, 1365175, 1366175, 1366275, 1366285,
				1366290, 1366295, 1366300, 1366465, 1366546, 1366580 },
			{ 30, 46030, 46029, 526029, 606029, 646029, 665237, 738286, 745486,
				745506, 745505, 745736, 748036, 748108, 748128, 748980,
				748979, 1468979, 1555379, 1555406, 1557707, 1557706,
				3357706, 3357748, 3357749, 3357748, 3361748, 3361749,
				3391749, 3391748, 3391747, 3391987, 3391986, 3391985,
				3391984, 3391983, 3391982, 3392782, 3392781, 3422781,
				3422804, 3422803, 3422802, 3422801, 3422800, 3422823,
				3446823, 3446822, 3446821, 3446829, 3446828, 3446827,
				3446826, 3446825, 3446826, 3458826, 3458825, 3459285,
				3459284, 3459283, 3459284, 3459384, 3460384, 3460484,
				3460494, 3460499, 3460504, 3460509, 3460779, 3460914,
				3460956 },
			{ 30, 50030, 50029, 1650029, 1750029, 1800029, 1819237, 1892286,
				1899486, 1899506, 1899505, 1899756, 1902256, 1902331,
				1902351, 1903302, 1903301, 4303301, 4389701, 4389731,
				4392233, 4392232, 10392232, 10392280, 10392281, 10392280,
				10397280, 10397281, 10497281, 10497280, 10497279, 10497519,
				10497518, 10497517, 10497516, 10497515, 10497514, 10498514,
				10498513, 10598513, 10598538, 10598537, 10598536, 10598535,
				10598534, 10598559, 10778559, 10778558, 10778557, 10778566,
				10778565, 10778564, 10778563, 10778562, 10778563, 10818563,
				10818562, 10819062, 10819061, 10819060, 10819061, 10819161,
				10820161, 10820261, 10820271, 10820276, 10820281, 10820286,
				10820781, 10821026, 10821073 },
			{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 10, 20, 120, 130, 130, 130, 145, 155, 155, 155,
				155, 155, 170, 170, 170, 220, 220, 220, 220, 220, 270, 270,
				270, 270, 280, 280, 280, 280, 280, 380, 380, 380, 430, 430,
				430, 480, 480, 480, 480, 480, 480, 480, 480, 480, 480, 480, } };

	@Override
	public List<TpcdsPromotedProduct> getPromotedProds() {
		// TODO Auto-generated method stub
		return null;
	}

	protected Date stringToDate(String date) {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		try {
			return formatter.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	
	public void genTBLonHDFS(Config conf, float targetGB, String table_name) {
		String single_tbl_gen_script = conf.getProp().get(
				BigConfConstants.BIGFRAME_GEN_SINGLETBL_SCRIPT);
		// System.out.println("gen promotion tbl script:" +
		// promt_tbl_gen_script);
		String singleTBLgen_script_path = (new File(single_tbl_gen_script))
				.getParentFile().getAbsolutePath();

		String cmd = "perl " + single_tbl_gen_script + " " + targetGB + " " + table_name;

		try {
			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec(cmd, null, new File(singleTBLgen_script_path));
			proc.waitFor();

		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
		
		//genTable(conf, targetGB, table_name);

		String tbl_file = singleTBLgen_script_path + "/" + "dsdgen" + "/"
				+ table_name + ".dat";

		Path local_path = new Path(tbl_file);

		try {
			Path hdfs_path = new Path(table_name + ".dat");
			Configuration config = new Configuration();
			config.addResource(new Path(conf.getProp().get(
					BigConfConstants.BIGFRAME_HADOOP_HOME)
					+ "/conf/core-site.xml"));
			FileSystem fileSystem = FileSystem.get(config);
			if (fileSystem.exists(hdfs_path)) {
				fileSystem.delete(hdfs_path, true);
			}

			fileSystem.copyFromLocalFile(local_path, hdfs_path);

		} catch (Exception e) {
			e.printStackTrace();
		}

		cleanUp(singleTBLgen_script_path, "dsdgen");
	}

	public void setItemResult(BufferedReader in, TpcdsItemInfo item_info) {
		String line;

		ArrayList<String> item_ids = new ArrayList<String>();
		ArrayList<String> prod_names = new ArrayList<String>();

		try {
			while ((line = in.readLine()) != null) {
				String[] fields = line.split("\\|", -1);

				String item_id = fields[1];
				String prod_name = fields[21];
				
				

				item_ids.add(item_id);
				prod_names.add(prod_name);
			}

			item_info.setItemIDs(item_ids);
			item_info.setProdNames(prod_names);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setPromtResult(BufferedReader in, TpcdsPromotionInfo promt_info) {
		String line;

		ArrayList<Integer> promotionSKs = new ArrayList<Integer>();
		ArrayList<String> promotionIDs = new ArrayList<String>();
		ArrayList<Integer> dateBeginSKs = new ArrayList<Integer>();
		ArrayList<Integer> dateEndSKs = new ArrayList<Integer>();
		ArrayList<Integer> productSKs = new ArrayList<Integer>();

		try {
			while ((line = in.readLine()) != null) {
				String[] fields = line.split("\\|", -1);

				String promtSK = fields[0];
				String promtID = fields[1];
				String datebegSK = fields[2];
				String dateendSK = fields[3];
				String prodSK = fields[4];

				if (promtSK.equals("") || promtID.equals("")
						|| datebegSK.equals("") || dateendSK.equals("")
						|| prodSK.equals("")) {
					continue;
				}

				if (CollectTPCDSstat.getDateBySK(Integer.parseInt(dateendSK)) != null
						&& CollectTPCDSstat
						.getDateBySK(Integer.parseInt(dateendSK))
						.before(stringToDate(RawTweetGenConstants.TWEET_BEGINDATE))) {
					continue;
				}

				if (CollectTPCDSstat.getDateBySK(Integer.parseInt(datebegSK)) != null
						&& CollectTPCDSstat.getDateBySK(Integer.parseInt(datebegSK))
						.after(stringToDate(RawTweetGenConstants.TWEET_ENDDATE))) {
					continue;
				}

				promotionSKs.add(Integer.parseInt(promtSK));
				promotionIDs.add(promtID);
				dateBeginSKs.add(Integer.parseInt(datebegSK));
				dateEndSKs.add(Integer.parseInt(dateendSK));
				productSKs.add(Integer.parseInt(prodSK));
			}

			promt_info.setDateBeginSK(dateBeginSKs);
			promt_info.setDateEndSK(dateEndSKs);
			promt_info.setPromotionSK(promotionSKs);
			promt_info.setProductSK(productSKs);
			promt_info.setPromotionID(promotionIDs);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void collectHDFSPromtTBL(Configuration mapreduce_config,
			String tbl_file, TpcdsPromotionInfo promt_info) {
		try {

			Path hdfs_path = new Path(tbl_file);
			FileSystem fileSystem = FileSystem.get(mapreduce_config);
			if (!fileSystem.exists(hdfs_path)) {
				LOG.error("Cannot find " + tbl_file + " on HDFS");
				System.exit(-1);
			} else {
				LOG.info("Got " + tbl_file + " on HDFS");
			}

			BufferedReader in = new BufferedReader(new InputStreamReader(
					fileSystem.open(hdfs_path)));
			setPromtResult(in, promt_info);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void collectHDFSItemTBL(Configuration mapreduce_config,
			String tbl_file, TpcdsItemInfo item_info) {
		try {

			Path hdfs_path = new Path(tbl_file);
			FileSystem fileSystem = FileSystem.get(mapreduce_config);
			if (!fileSystem.exists(hdfs_path)) {
				LOG.error("Cannot find " + tbl_file + " on HDFS");
				System.exit(-1);
			} else {
				LOG.info("Got " + tbl_file + " on HDFS");
			}

			BufferedReader in = new BufferedReader(new InputStreamReader(
					fileSystem.open(hdfs_path)));
			setItemResult(in, item_info);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void cleanUp(String working_dir, String deleted_dir) {
		String cmd = "rm -r " + deleted_dir;

		try {
			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec(cmd, null, new File(working_dir));
			proc.waitFor();
			/*
			 * InputStream stderr = proc.getErrorStream(); InputStream stdout =
			 * proc.getInputStream();
			 * 
			 * 
			 * InputStreamReader isr = new InputStreamReader(stderr);
			 * BufferedReader br = new BufferedReader(isr); InputStreamReader
			 * isout = new InputStreamReader(stdout); BufferedReader br1 = new
			 * BufferedReader(isout);
			 * 
			 * 
			 * String line = null;
			 */

		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}

//	public PromotionInfo getPromotInfo(Config conf, int targetGB) {
//		PromotionInfo promt_info = new PromotionInfo();
//
//		genPromtTBLlocal(conf, targetGB, promt_info);
//
//		return promt_info;
//	}

	private int getScaleSlot(float targetGB) {
		int i = 0;

		for (; targetGB > arScaleVolume[i]; i++) {
			;
		}

		return i;
	}

	private int dist_weight(int index, int wset) {
		int res;

		res = TPCDS_ROWCOUNT_WEIGHTSET[wset - 1][index - 1];

		if (index > 1) {
			res -= TPCDS_ROWCOUNT_WEIGHTSET[wset - 1][index - 2];
		}

		return res;
	}

	private long logScale(int tableNum, float targetGB) {
		long rowCount = 0;
		int index = 1, delta;
		float Offset;

		int i = getScaleSlot(targetGB);

		delta = dist_weight(tableNum + 1, i + 1) - dist_weight(tableNum + 1, i);
		Offset = (float) (targetGB - arScaleVolume[i - 1])
				/ (float) (arScaleVolume[i] - arScaleVolume[i - 1]);
		rowCount = (int) (Offset * delta);
		rowCount += dist_weight(tableNum + 1, index);

		return rowCount;
	}

	@Override
	public long getNumOfCustomer(float targetGB) {
		return get_rowcount(4, targetGB) * 1000;
	}

	@Override
	public long getNumOfItem(float targetGB) {
		return get_rowcount(11, targetGB) * 2000;
	}

	private long get_rowcount(int table, float targetGB) {
		long baseRowCount = 0;
		switch ((int)targetGB) {
		case 100000:
			baseRowCount = dist_weight(table + 1, 9);
			break;
		case 30000:
			baseRowCount = dist_weight(table + 1, 8);
			break;
		case 10000:
			baseRowCount = dist_weight(table + 1, 7);
			break;
		case 3000:
			baseRowCount = dist_weight(table + 1, 6);
			break;
		case 1000:
			baseRowCount = dist_weight(table + 1, 5);
			break;
		case 300:
			baseRowCount = dist_weight(table + 1, 4);
			break;
		case 100:
			baseRowCount = dist_weight(table + 1, 3);
			break;
		case 10:
			baseRowCount = dist_weight(table + 1, 2);
			break;
		case 1:
			baseRowCount = dist_weight(table + 1, 1);
			break;
		default:
			baseRowCount = logScale(table, targetGB);
		}
		return baseRowCount;
	}

	@Override
	public long[] getCustTwitterAcc(float tpcds_targetGB, float graph_targetGB) {
		// TODO Auto-generated method stub
		int num_customer = (int) getNumOfCustomer((int) tpcds_targetGB);
		int num_twitter_user = (int) KroneckerGraphGen
				.getNodeCount(graph_targetGB);

		RandomEngine twister = new MersenneTwister(RandomSeeds.SEEDS_TABLE[0]);

		long[] customers = new long[num_customer / 2];
		long[] customer_twitterAcc = new long[num_customer / 2];

		RandomSampler.sample(num_customer / 2, num_customer, num_customer / 2,
				1, customers, 0, twister);
		RandomSampler.sample(num_customer / 2, num_twitter_user,
				num_customer / 2, 1, customer_twitterAcc, 0, twister);

		/*
		 * long [] cust_acc = new long [customer_account.size()]; int index = 0;
		 * for (Integer acc : customer_account) { cust_acc[index] = acc;
		 * index++; }
		 */
		return customer_twitterAcc;
	}

	@Override
	public long[] getNonCustTwitterAcc(long[] customer_twitterAcc,
			int num_twitter_user) {

		Set<Long> customer_twitterAcc_set = new HashSet<Long>();
		for (int i = 0; i < customer_twitterAcc.length; i++) {
			customer_twitterAcc_set.add(customer_twitterAcc[i]);
			// System.out.println(customer_twitterAcc[i]);
		}

		/**
		 * Here is dangerous. May cause overflow!
		 */
		long[] non_cust_acc = new long[num_twitter_user
		                               - customer_twitterAcc.length];
		// System.out.println(num_customer);
		int index = 0;
		for (Long i = 1L; i <= num_twitter_user; i++) {
			if (!customer_twitterAcc_set.contains(i)) {
				non_cust_acc[index] = i;
				// System.out.println(i);
				index++;

			}
		}

		return non_cust_acc;
	}

	
}
