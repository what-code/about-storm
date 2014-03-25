package group.test;

import java.io.Serializable;
import java.util.*;

public class Bean{
	private int id;
	private String name;
	private Date bd;
	private List list;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Date getBd() {
		return bd;
	}
	public void setBd(Date bd) {
		this.bd = bd;
	}
	public List getList() {
		return list;
	}
	public void setList(List list) {
		this.list = list;
	}
}
