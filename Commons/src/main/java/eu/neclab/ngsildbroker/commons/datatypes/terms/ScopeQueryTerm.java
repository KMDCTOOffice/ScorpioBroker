package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;

public class ScopeQueryTerm implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3852848071227315655L;
	private static final String REGEX_PLUS = "[^\\/]+";
	private static final String REGEX_HASH = ".*";
	private String[] scopeLevels = null;
	private ScopeQueryTerm prev = null;
	private ScopeQueryTerm next = null;
	private boolean nextAnd = true;
	private ScopeQueryTerm firstChild = null;
	private ScopeQueryTerm parent = null;
	private String scopeQueryString;
	
	public ScopeQueryTerm() {
		// for serialization
	}

	public String[] getScopeLevels() {
		return scopeLevels;
	}

	public void setScopeLevels(String[] scopeLevels) {
		this.scopeLevels = scopeLevels;
	}

	public boolean hasNext() {
		return next != null;
	}

	public Map<String, Map<String, Object>> calculateQuery(List<Map<String, Object>> queryResult) {
		Iterator<Map<String, Object>> it = queryResult.iterator();
		Map<String, Map<String, Object>> removed = Maps.newHashMap();
		while (it.hasNext()) {
			Map<String, Object> entity = it.next();
			List<String[]> scopes = EntityTools.getScopes(entity);
			if (!calculate(scopes)) {
				removed.put((String) entity.get(NGSIConstants.JSON_LD_ID), entity);
				it.remove();
			}
		}
		return removed;
	}

	public boolean calculate(List<String[]> scopes) {
		boolean result = false;
		if (firstChild == null) {
			result = calculateMe(scopes);
		} else {
			result = firstChild.calculate(scopes);
		}
		if (hasNext()) {
			if (nextAnd) {
				result = result && next.calculate(scopes);
			} else {
				result = result || next.calculate(scopes);
			}
		}

		return result;
	}

	private boolean calculateMe(List<String[]> entryScopes) {
		for (String[] entryScope : entryScopes) {
			int lenEntryScope = entryScope.length - 1;
			boolean result = false;
			for (int i = 0; i < scopeLevels.length; i++) {
				String scopeLevel = scopeLevels[i];
				if (scopeLevel.equals("#")) {
					result = true;
					break;
				}
				if (i > lenEntryScope) {
					result = false;
					break;
				}
				if (i == lenEntryScope) {
					result = true;
				}
				if (scopeLevel.equals("+") || scopeLevel.equals(entryScope[i])) {
					continue;
				}
				result = false;
				break;
			}
			if (result) {
				return true;
			}
		}
		return false;
	}

	public void toSql(StringBuilder result) {
		if (firstChild != null) {
			result.append("(");
			firstChild.toSql(result);
			result.append(")");
		} else {
			result.append("matchScope(" + DBConstants.DBCOLUMN_SCOPE + ",'^");
			for (String entry : scopeLevels) {
				switch (entry) {
				case "+":
					result.append("\\/");
					result.append(REGEX_PLUS);
					break;
				case "#":
					result.append(REGEX_HASH);
					break;
				default:
					result.append("\\/");
					result.append(entry);
					break;
				}
			}
			result.append("$'");
			result.append(")");
		}
		if (hasNext()) {
			if (nextAnd) {
				result.append(" and ");
			} else {
				result.append(" or ");
			}
			next.toSql(result);
		}
	}
	
	public String getSQLScopeQuery() {
		StringBuilder result = new StringBuilder("'^");
		for (String entry : scopeLevels) {
			switch (entry) {
			case "+":
				result.append("\\/");
				result.append(REGEX_PLUS);
				break;
			case "#":
				result.append(REGEX_HASH);
				break;
			default:
				result.append("\\/");
				result.append(entry);
				break;
			}
		}
		result.append("$'");
		return result.toString();
	}
	
	public void toSql(StringBuilder result, StringBuilder followUp) {
		if (firstChild != null) {
			result.append("(");
			followUp.append("(");
			firstChild.toSql(result);
			result.append(")");
			followUp.append(")");
		} else {
			result.append("matchScope(" + DBConstants.DBCOLUMN_SCOPE + ",'^");
			followUp.append("matchScope(" + DBConstants.DBCOLUMN_SCOPE + ",''^");
			for (String entry : scopeLevels) {
				switch (entry) {
				case "+":
					result.append("\\/");
					result.append(REGEX_PLUS);
					break;
				case "#":
					result.append(REGEX_HASH);
					break;
				default:
					result.append("\\/");
					result.append(entry);
					break;
				}
			}
			result.append("$'");
			followUp.append("$''");
			followUp.append(")");
			result.append(")");
		}
		if (hasNext()) {
			if (nextAnd) {
				result.append(" and ");
				followUp.append(" and ");
			} else {
				result.append(" or ");
				followUp.append(" or ");
			}
			next.toSql(result);
		}
	}

	public ScopeQueryTerm getNext() {
		return next;
	}

	public void setNext(ScopeQueryTerm next) {
		this.next = next;
		next.parent = parent;
		next.prev = this;
	}

	public boolean isNextAnd() {
		return nextAnd;
	}

	public void setNextAnd(boolean nextAnd) {
		this.nextAnd = nextAnd;
	}

	public ScopeQueryTerm getFirstChild() {
		return firstChild;
	}

	public void setFirstChild(ScopeQueryTerm firstChild) {
		this.firstChild = firstChild;
	}

	public ScopeQueryTerm getParent() {
		return parent;
	}

	public void setParent(ScopeQueryTerm parent) {
		this.parent = parent;
	}

	public ScopeQueryTerm getPrev() {
		return prev;
	}

	public boolean calculateEntity(Map<String, Object> entity) {
		return calculate(EntityTools.getScopes(entity));
	}

	public String getScopeQueryString() {
		return scopeQueryString;
	}

	public void setScopeQueryString(String scopeQueryString) {
		this.scopeQueryString = scopeQueryString;
	}
	
	

}
