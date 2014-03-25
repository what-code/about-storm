package group.test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MinuteGeneratorTasks extends TimerTask{
	private static Log loger = LogFactory.getLog(MinuteGeneratorNew.class);
	public static void doTask(){
		String tableName = "pluginLog_js";
		//定时任务执行时间
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 1);

		//昨天
		Date date = new Date(calendar.getTime().getTime() - 24l*60*60*1000);
		SimpleDateFormat sd = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sd1 = new SimpleDateFormat("yyyy-MM-dd");
		String yesterday = sd.format(date);
		//现在
		Date now = new Date();
		
		loger.info("MinuteGeneratorTasks--now-->" + now + "--tasks--" + yesterday);
		try {
			MinuteGeneratorNew mn = new MinuteGeneratorNew(sd.format(date));
			mn.getSummaryData(tableName, sd1.format(date));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Override
	public void run() {
		doTask();
	}
	
	public static void main(String[] args){
		doTask();
	}
}
