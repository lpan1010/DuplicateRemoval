package com.mapbar.deduplicate;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;

import com.mapbar.deduplicate.handler.DuplicateHandler;
import com.mapbar.deduplication.conf.Configuration;
import com.mapbar.deduplicate.util.ReadDirections;
import com.mapbar.deduplicate.util.ReadFile;
import com.mapbar.entity.POIEntity;
import com.mapbar.entity.POIEntityParsed;
import com.mapbar.util.common.FileUtil;

/***
 * 
 * @author liupa
 *
 */
public class DeDuplication {
	private static final Logger LOG = Logger.getLogger(DeDuplication.class);
	
	public static String currentDir = new String();
	
	public static Configuration conf = Configuration.getInstance();
	/**构造函数*/
	public DeDuplication(){
		init();
	}
	
	public void init(){
		ResourceBundle rb = ResourceBundle.getBundle("deduplication");
		conf.setInputPath(rb.getString("input_path"));
		conf.setCfgPath(rb.getString("cfg_path"));
		conf.setOutputPath(rb.getString("output_path"));
		conf.setNThreadParser(Integer.parseInt(rb.getString("NThreadParser")));
		conf.setNThreadSearcher(Integer.parseInt(rb.getString("NThreadSearcher")));
		conf.setNThreadDiscriminator(Integer.parseInt(rb.getString("NThreadDiscriminator")));
		conf.setRunType(rb.getString("run_type"));
		conf.setSourceExtension(rb.getString("source_extension"));
		conf.setTargetExtension(rb.getString("target_extension"));
		conf.setUser(rb.getString("user"));
		conf.setThreshold(Double.parseDouble(rb.getString("threshold")));
		conf.setSEARCH_RESULT_SIZE(Integer.parseInt(rb.getString("search_result_size")));
		
		System.out.println(conf.getInputPath() + "\t" + conf.getCfgPath());
	}
	/**
	 * 
	 * @param start_time
	 * @param end_time
	 * @return
	 */
	public static String printTime(long start_time, long end_time){
		long time = (end_time - start_time);
		long hour = time/3600000;
		long min = (time%3600000)/60000;
		long second = (time%3600000)%60000/1000;
		String timeString = hour + "小时" + min + "分钟" + second + "秒";
		return timeString;
	}
	
	public void start(Configuration conf){
		String run_type = conf.getRunType();
		ReadFile rf = new ReadFile();
		/**目录列表，读取根目录下各个城市的目录*/
		ArrayList<File> dirs = ReadDirections.readDir(conf.getInputPath(), conf.getSourceExtension());
		/**目录下的文件列表，包含source和target*/
		ArrayList<File> sourcefiles = new ArrayList<File>();
		ArrayList<File> targetfiles = new ArrayList<File>();
		/**文件中包含的数据转化为POIEntity对象列表*/
		ArrayList<POIEntity> source_entity_list = new ArrayList<POIEntity>();
		ArrayList<POIEntity> target_entity_list = new ArrayList<POIEntity>();
		/**互判重类型***/
		if(run_type.equalsIgnoreCase("hpc")){
			LOG.info("判重类型 == 互判重");
			/**遍历每个目录*/
			if(dirs.size() == 0){
				LOG.info("目录为空," + conf.getInputPath());
			}
			for(int i = 0; i < dirs.size(); i++){
				LOG.info("目录 == " + dirs.get(i));
			}
			for(int i = 0,len = dirs.size(); i < len; i++){
				LOG.info("\n\n---------------------------------------------------------------");
				LOG.info("正在读取目录 == " + dirs.get(i));
				/**读入目录下的源数据*/
				currentDir = dirs.get(i).getAbsolutePath();
				LOG.info("currentDir: "+currentDir);
				sourcefiles = FileUtil.listFiles(dirs.get(i), FileUtil.getEndsWithFilter(conf.getSourceExtension()));
				source_entity_list = rf.readPOIFile(sourcefiles);
				
				/**读入目录下的目标数据**/
				targetfiles = FileUtil.listFiles(dirs.get(i), FileUtil.getEndsWithFilter(conf.getTargetExtension()));
				target_entity_list = rf.readPOIFile(targetfiles);
				LOG.info("数据集合一：" + source_entity_list.size() + " 数据集合二：" + target_entity_list.size());
				if(source_entity_list.size() != 0 && target_entity_list.size() != 0){
					
					deduplication(source_entity_list, target_entity_list, conf);
				}
				else{
					LOG.info("数据集合大小为0");
					continue;
				}
			}
		}
		/**自判重类型**/
		else if(run_type.equalsIgnoreCase("zpc")){
			LOG.info("判重类型 == 自判重");
			LOG.info("\n\n--------------------------------------------------------------------------");
			for(int i = 0, len = dirs.size();  i < len; i++){
				LOG.info("正在读取目录 == " + dirs.get(i));
				/**读入目录下的源数据*/
				sourcefiles = FileUtil.listFiles(dirs.get(i), FileUtil.getEndsWithFilter(conf.getSourceExtension()));
				source_entity_list = rf.readPOIFile(sourcefiles);
				LOG.info("数据集合一：" + source_entity_list.size() + " 数据集合二：" + target_entity_list.size());
				if(source_entity_list.size() != 0){
					deduplication(source_entity_list, conf);
				}
				else{
					LOG.info("数据集合大小为0");
					continue;
				}
			}
		}
	}
	/***
	 * 只有一个数据参数的接口，可用做自判重
	 * @param source
	 * @param conf
	 * @return
	 */
	public HashMap<Double, ArrayList<POIEntityParsed>> deduplication(ArrayList<POIEntity> source, Configuration conf){
		HashMap<Double, ArrayList<POIEntityParsed>> resultSet = new HashMap<Double, ArrayList<POIEntityParsed>>();
		resultSet = deduplication(source, null, conf);
		return resultSet;
	}
	/**
	 * 包含两个数据参数的接口，可用作自判重，也可用作互判重
	 * @param source
	 * @param target
	 * @param conf
	 * @return
	 */
	public HashMap<Double, ArrayList<POIEntityParsed>> deduplication(ArrayList<POIEntity> source, ArrayList<POIEntity> target, Configuration conf){
		HashMap<Double, ArrayList<POIEntityParsed>> groups = new HashMap<Double, ArrayList<POIEntityParsed>>();
		/**开始时间*/
		long start_time = System.currentTimeMillis();
		/**判重并返回结果*/
		DuplicateHandler dHandler = new DuplicateHandler();
		groups = dHandler.work(source, target, conf);
		dHandler = null;
		/**结束时间*/
		long end_time = System.currentTimeMillis();
		LOG.info("判重用时:" + printTime(start_time, end_time));
		return groups;
	}
	
	public static void main(String[] args){
		/**开始时间*/
		long start_time = System.currentTimeMillis();
		DeDuplication du = new DeDuplication();
		du.start(conf);
		/**结束时间*/
		long end_time = System.currentTimeMillis();
		LOG.info("判重总用时:" + printTime(start_time, end_time));
	}
}
