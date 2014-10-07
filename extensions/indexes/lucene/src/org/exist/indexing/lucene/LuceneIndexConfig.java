package org.exist.indexing.lucene;

import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.exist.dom.ElementImpl;
import org.exist.dom.QName;
import org.exist.storage.ElementValue;
import org.exist.storage.NodePath;
import org.exist.util.DatabaseConfigurationException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class LuceneIndexConfig {

    private final static String N_INLINE = "inline";
    private final static String N_IGNORE = "ignore";

    private static final String QNAME_ATTR = "qname";
    private static final String MATCH_ATTR = "match";

    private final static String IGNORE_ELEMENT = "ignore";
    private final static String INLINE_ELEMENT = "inline";
	private static final String FIELD_ATTR = "field";
	private static final String TYPE_ATTR = "type";

    private String name = null;

    private NodePath path = null;

    private boolean isQNameIndex = false;

    private Map<QName, String> specialNodes = null;

    private LuceneIndexConfig nextConfig = null;
    
    private FieldType type = null;
    
    // this is for the @attr matching
    // perhaps in the future it could do a proper predicate check instead 
    private String matchAttributeName = null; 
    private String matchAttributeValue = null;
    private float matchAttributeBoost = -1.0f;
    
    public LuceneIndexConfig(Element config, Map<String, String> namespaces, AnalyzerConfig analyzers,
    			Map<String, FieldType> fieldTypes) throws DatabaseConfigurationException {
        if (config.hasAttribute(QNAME_ATTR)) {
            QName qname = parseQName(config, namespaces);
            path = new NodePath(qname);
            isQNameIndex = true;
        } else {
            String matchPath = config.getAttribute(MATCH_ATTR);

            try {
				path = new NodePath(namespaces, matchPath);
				if (path.length() == 0)
				    throw new DatabaseConfigurationException("Lucene module: Invalid match path in collection config: " +
				        matchPath);
			} catch (IllegalArgumentException e) {
				throw new DatabaseConfigurationException("Lucene module: invalid qname in configuration: " + e.getMessage());
			}
        }

        // match attr boosting configuration
        if (config.hasAttribute("match-attr-name")) {
            matchAttributeName = config.getAttribute("match-attr-name");

            String boostAttr = config.getAttribute("match-attr-boost");
            // blatant copy from FieldType constructor
            if (boostAttr != null && boostAttr.length() > 0) {
                try {
                    matchAttributeBoost = Float.parseFloat(boostAttr);
                } catch (NumberFormatException e) {
                    throw new DatabaseConfigurationException("Invalid value for attribute 'boost'. Expected float, "
                            + "got: " + boostAttr);
                }
            }
            
            if (config.hasAttribute("match-attr-value"))
                matchAttributeValue = config.getAttribute("match-attr-value");
            else
                matchAttributeValue = null;
        } else
            matchAttributeName = null;


        String name = config.getAttribute(FIELD_ATTR);
        if (name != null && name.length() > 0)
        	setName(name);
        
        String fieldType = config.getAttribute(TYPE_ATTR);
        if (fieldType != null && fieldType.length() > 0)
        	type = fieldTypes.get(fieldType);        
        if (type == null)
        	type = new FieldType(config, analyzers);

        parse(config, namespaces);
    }

    private void parse(Element root, Map<String, String> namespaces) throws DatabaseConfigurationException {
        Node child = root.getFirstChild();
        while (child != null) {
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                final String localName = child.getLocalName();
                if (null != localName) {
                    switch (localName) {
                        case IGNORE_ELEMENT: {
                            String qnameAttr = ((Element) child).getAttribute(QNAME_ATTR);
                            if (StringUtils.isEmpty(qnameAttr)) {
                                throw new DatabaseConfigurationException("Lucene configuration element 'ignore' needs an attribute 'qname'");
                            }
                            if (specialNodes == null) {
                                specialNodes = new TreeMap<>();
                            }
                            specialNodes.put(parseQName(qnameAttr, namespaces), N_IGNORE);
                            break;
                        }
                        case INLINE_ELEMENT: {
                            String qnameAttr = ((Element) child).getAttribute(QNAME_ATTR);
                            if (StringUtils.isEmpty(qnameAttr)) {
                                throw new DatabaseConfigurationException("Lucene configuration element 'inline' needs an attribute 'qname'");
                            }
                            if (specialNodes == null) {
                                specialNodes = new TreeMap<>();
                            }
                            specialNodes.put(parseQName(qnameAttr, namespaces), N_INLINE);
                            break;
                        }
                    }
                }
            }
            child = child.getNextSibling();
        }
    }

    // return saved Analyzer for use in LuceneMatchListener
    public Analyzer getAnalyzer() {
        return type.getAnalyzer();
    }

    public String getAnalyzerId() {
        return type.getAnalyzerId();
    }

    public QName getQName() {
        return path.getLastComponent();
    }

    public NodePath getNodePath() {
        return path;
    }

    public float getBoost() {
        return type.getBoost();
    }
    
    public float getBoost(ElementImpl element) {
        // do we have attribute match boosting? else return normal boost
        if (element != null && matchAttributeName != null && matchAttributes(element)) {
            return matchAttributeBoost;
        }
        else return getBoost();
    }

    public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
	
	public void add(LuceneIndexConfig config) {
		if (nextConfig == null)
			nextConfig = config;
		else
			nextConfig.add(config);
	}
	
	public LuceneIndexConfig getNext() {
		return nextConfig;
	}
	
	/**
	 * @return true if this index can be queried by name
	 */
	public boolean isNamed() {
		return name != null;
	}

	public boolean isIgnoredNode(QName qname) {
        return specialNodes != null && specialNodes.get(qname) == N_IGNORE;
    }

    public boolean isInlineNode(QName qname) {
        return specialNodes != null && specialNodes.get(qname) == N_INLINE;
    }

    public static QName parseQName(Element config, Map<String, String> namespaces) throws DatabaseConfigurationException {
        String name = config.getAttribute(QNAME_ATTR);
        if (StringUtils.isEmpty(name))
            throw new DatabaseConfigurationException("Lucene index configuration error: element " + config.getNodeName() +
                    " must have an attribute " + QNAME_ATTR);

        return parseQName(name, namespaces);
    }

    protected static QName parseQName(String name, Map<String, String> namespaces) throws DatabaseConfigurationException {
        boolean isAttribute = false;
        if (name.startsWith("@")) {
            isAttribute = true;
            name = name.substring(1);
        }
        try {
            String prefix = QName.extractPrefix(name);
            String localName = QName.extractLocalName(name);
            String namespaceURI = "";
            if (prefix != null) {
                namespaceURI = namespaces.get(prefix);
                if(namespaceURI == null) {
                    throw new DatabaseConfigurationException("No namespace defined for prefix: " + prefix +
                            " in index definition");
                }
            }
            QName qname = new QName(localName, namespaceURI, prefix);
            if (isAttribute)
                qname.setNameType(ElementValue.ATTRIBUTE);
            return qname;
        } catch (IllegalArgumentException e) {
            throw new DatabaseConfigurationException("Lucene index configuration error: " + e.getMessage(), e);
        }
    }

    private boolean matchAttributes(ElementImpl element) {
        if (matchAttributeName == null)
            return true;

        // note: if are in the process of storing this element, there wont be any attributes and we will get null pointer ex.
        if (element.hasAttribute(matchAttributeName)) {
            // ok, found attribute on element. check attribute value
            if (matchAttributeValue != null)
                return element.getAttribute(matchAttributeName).equals(matchAttributeValue);
            else
                return true;
        } else
            return false;
    }

    public boolean match(NodePath other) {
        if (isQNameIndex) {
            final QName qn1 = path.getLastComponent();
            final QName qn2 = other.getLastComponent();
            return qn1.getNameType() == qn2.getNameType() && qn2.equalsSimple(qn1);
        }
        return path.match(other);
    }

	@Override
	public String toString() {
		return path.toString();
	}
}

