package bigframe.bigif;

import java.util.HashSet;
import java.util.Set;

import bigframe.util.Config;


public class QuerygenConf extends Config {
	protected Set<String> queryVariety;
	protected Set<String> queryVelocity;
	protected Integer queryVolumn;
	
	public QuerygenConf() {
		super();
		queryVariety = new HashSet<String>();
		queryVelocity = new HashSet<String>();
	}
	
	/*
	public void addQueryVariety(String type) {
		queryVariety.add(type);
	}
	
	public void addQueryVelocity(String type) {
		queryVelocity.add(type);
	}
	*/
	
	public Set<String> getQueryVariety() {
		return queryVariety;
	}
	
	public Integer getQueryVolumn() {
		return queryVolumn;
	}
	
	public Set<String> getQueryVelocity() {
		return queryVelocity;
	}
	
	/*
	public void setQueryVariety(Set<String> qVariety) {
		queryVariety = qVariety;
	}

	
	public void setQueryVolumn(Integer qVolumn) {
		queryVolumn = qVolumn;
	}
	
	public void setQueryVelocity(Set<String> qVelocity) {
		queryVelocity = qVelocity;
	}
	*/
	@Override
	public void printConf() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void reloadConf() {
		// TODO Auto-generated method stub
		
	}
	
}
