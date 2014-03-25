package group.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MinuteGeneratorServlet extends HttpServlet {

	private Log loger = LogFactory.getLog(MinuteGeneratorNew.class);

	/**
	 * get
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doPost(request, response);
	}

	/**
	 * post
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// 定时任务执行时间
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 1);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		Date start = calendar.getTime();
		//现在
		Date now = new Date();
		//超过定时任务时间,明天1点开始执行
		if(now.getTime() > start.getTime()){
			start = new Date(calendar.getTime().getTime() + 24l*60*60*1000);
		}
		loger.info("MinuteGeneratorServlet--now-->" + now + "--tasks--" + start);
		new Timer().schedule(new MinuteGeneratorTasks(),start, 24*60*60*1000);
	}
}
