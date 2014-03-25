package com.b5m.plugin.util;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * 多数据源包装类
 * 
 * @author xiao.zhao
 * 
 */
public class MultiDataSource implements DataSource, ApplicationContextAware {
	public static final String BARCODE_DATASOURCE = "barcodeDataSource";
	public static final String Mobile_DATASOURCE = "mobileDataSource";
	public static final String COMM_DataSource = "commDataSource";
	private ApplicationContext applicationContext = null;

	private DataSource getDataSource() {
		String name = MultiDataSourceUtils.getDataSourceName();
		if (StringUtils.isEmpty(name)) {
			name = MultiDataSource.Mobile_DATASOURCE;
		}
		DataSource source = applicationContext.getBean(name, DataSource.class);
		return source;
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return this.getDataSource().getLogWriter();
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		return this.getDataSource().getLoginTimeout();
	}

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {
		this.getDataSource().setLogWriter(out);

	}

	@Override
	public void setLoginTimeout(int tiemOut) throws SQLException {
		this.getDataSource().setLoginTimeout(tiemOut);

	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return this.getDataSource().isWrapperFor(iface);
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		return null;
	}

	@Override
	public Connection getConnection() throws SQLException {
		Connection conn = this.getDataSource().getConnection();
		return conn;
	}

	@Override
	public Connection getConnection(String username, String password)
			throws SQLException {

		return this.getDataSource().getConnection(username, password);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = applicationContext;

	}

}
