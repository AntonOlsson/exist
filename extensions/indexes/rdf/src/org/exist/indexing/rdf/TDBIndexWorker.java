package org.exist.indexing.rdf;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdfxml.xmlinput.SAX2Model;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.sparql.resultset.ResultSetApply;
import com.hp.hpl.jena.sparql.util.graph.GraphUtils;
import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Logger;
import org.exist.collections.Collection;
import org.exist.dom.memtree.DocumentBuilderReceiver;
import org.exist.dom.persistent.AbstractCharacterData;
import org.exist.dom.persistent.AttrImpl;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.dom.persistent.DocumentSet;
import org.exist.dom.persistent.ElementImpl;
import org.exist.dom.persistent.IStoredNode;
import org.exist.dom.persistent.NodeProxy;
import org.exist.dom.persistent.NodeSet;
import org.exist.indexing.AbstractStreamListener;
import org.exist.indexing.IndexController;
import org.exist.indexing.IndexWorker;
import org.exist.indexing.MatchListener;
import org.exist.indexing.StreamListener;
import org.exist.numbering.NodeId;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.DBBroker;
import org.exist.storage.IndexSpec;
import org.exist.storage.NodePath;
import org.exist.storage.txn.Txn;
import org.exist.util.DatabaseConfigurationException;
import org.exist.util.Occurrences;
import org.exist.xquery.QueryRewriter;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.Sequence;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author Anton Olsson <abc2386@gmail.com>
 */
public class TDBIndexWorker implements IndexWorker {

    private static final Logger LOG = Logger.getLogger(TDBIndexWorker.class);

    private final DBBroker broker;
    private final TDBRDFIndex index;
    private DocumentImpl currentDoc;
    private Model cacheModel = GraphFactory.makeDefaultModel();

    private RDFIndexConfig config;
    private int mode;
    private final TDBStreamListener listener = new TDBStreamListener();
    private IndexController controller;
    private static final String CONFIG_ELEMENT_NAME = "rdf";

    TDBIndexWorker(TDBRDFIndex index, DBBroker broker) {
        this.index = index;
        this.broker = broker;
    }

    @Override
    public String getIndexId() {
        return TDBRDFIndex.ID;
    }

    @Override
    public String getIndexName() {
        return index.getIndexName();
    }

    @Override
    public Object configure(IndexController controller, NodeList configNodes, Map<String, String> namespaces) throws DatabaseConfigurationException {
        this.controller = controller;
        LOG.debug("Configuring TDB index...");

        for (int i = 0; i < configNodes.getLength(); i++) {
            Node node = configNodes.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE
                    && node.getLocalName().equals(CONFIG_ELEMENT_NAME)) {
                RDFIndexConfig tdbIndexConfig = new RDFIndexConfig((Element) node, namespaces);
                return tdbIndexConfig;
            }
        }

        return null;
    }

    private RDFIndexConfig getIndexConfig(Collection coll) {
        IndexSpec indexConf = coll.getIndexConfiguration(broker);
        if (indexConf != null) {
            return (RDFIndexConfig) indexConf.getCustomIndexSpec(getIndexId());
        }
        return null;
    }

    @Override
    public void setDocument(DocumentImpl doc) {
        RDFIndexConfig origConfig = getIndexConfig(doc.getCollection());
        if (origConfig != null) {
            currentDoc = doc;
            // Create a copy of the original RDFIndexConfig (there's only one per db instance), so we can safely work with it.
            this.config = new RDFIndexConfig(origConfig);
        } else {
            currentDoc = null;
            this.config = null;
        }
    }

    @Override
    public void setDocument(DocumentImpl document, int mode) {
        setDocument(document);
        setMode(mode);
    }

    @Override
    public void setMode(int mode) {
        this.mode = mode;
    }

    @Override
    public DocumentImpl getDocument() {
        return currentDoc;
    }

    @Override
    public int getMode() {
        return mode;
    }

    @Override
    public <T extends IStoredNode> IStoredNode getReindexRoot(IStoredNode<T> node, NodePath path, boolean insert, boolean includeSelf) {
        // return topmost parent element under root (tree level 2)
        NodeId nodeId = node.getNodeId();
        while (nodeId.getTreeLevel() > 2) {
            nodeId = nodeId.getParentId();
        }
        return broker.objectWith(node.getOwnerDocument(), nodeId);
    }

    @Override
    public StreamListener getListener() {
        if (currentDoc == null || mode == StreamListener.REMOVE_ALL_NODES)
            return null;

        listener.reset(currentDoc);
        return listener;
    }

    @Override
    public MatchListener getMatchListener(DBBroker broker, NodeProxy proxy) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void flush() {
        if (currentDoc == null)
            return;

        switch (mode) {
            case StreamListener.STORE:
                storeNodes();
                break;
            case StreamListener.REMOVE_ALL_NODES:
                removeDocument();
                break;
            case StreamListener.REMOVE_SOME_NODES:
                removeNodes();
                break;
            case StreamListener.REMOVE_BINARY:
//            	removePlainTextIndexes();
                break;
        }

        cacheModel.removeAll();
    }

    @Override
    public void removeCollection(Collection collection, DBBroker broker, boolean reindex) throws PermissionDeniedException {
        RDFIndexConfig cfg = getIndexConfig(collection);
        if (cfg != null) {
            Iterator<DocumentImpl> it = collection.iterator(broker);
            while (it.hasNext()) {
                DocumentImpl doc = it.next();
                removeDocument(doc);
            }
        }
    }

    @Override
    public boolean checkIndex(DBBroker broker) {
        return true;
    }

    private Model getOrCreateModel(DocumentImpl doc) {
        return index.getDataset().getNamedModel(doc.getDocumentURI());
    }

    private Model getModelOrNull(DocumentImpl doc) {
        final String documentURI = doc.getDocumentURI();

        if (index.getDataset().containsNamedModel(documentURI)) {
            return index.getDataset().getNamedModel(documentURI);
        } else
            return null;
    }

    @Override
    public Occurrences[] scanIndex(XQueryContext context, DocumentSet docs, NodeSet contextSet, Map<?, ?> hints) {
        for (Iterator<DocumentImpl> iDoc = docs.getDocumentIterator(); iDoc.hasNext();) {
            DocumentImpl doc = iDoc.next();
            Model model = getModelOrNull(doc);

            if (model == null)
                continue;

            Iterator<com.hp.hpl.jena.graph.Node> allNodes = GraphUtils.allNodes(model.getGraph());
            while (allNodes.hasNext()) {
                com.hp.hpl.jena.graph.Node next = allNodes.next();
//                if (next)
            }
        }

        Occurrences[] oc = new Occurrences[0];

        return oc;
    }

    @Override
    public QueryRewriter getQueryRewriter(XQueryContext context) {
        return null;
    }

    private void removeDocument() {
        removeDocument(currentDoc);
    }

    private void removeNodes() {
        if (cacheModel.isEmpty())
            return;
        Model model = getModelOrNull(currentDoc);
        if (model == null) {
            LOG.warn("removeNodes with null model");
            return;
        }
        model.remove(cacheModel);
    }

    private void storeNodes() {
        if (cacheModel.isEmpty())
            return;
        Model model = getOrCreateModel(currentDoc);
        model.add(cacheModel);
    }

    private void removeDocument(Document doc) {
        if (currentDoc != null) {
            String documentURI = currentDoc.getDocumentURI();
            Dataset dataset = index.getDataset();
            if (dataset.containsNamedModel(documentURI))
                dataset.removeNamedModel(documentURI);
        }
    }

    /**
     * Query TDB with SPARQL
     * @param queryString SPARQL query string
     * @return Result document
     * @throws org.exist.xquery.XPathException Query error
     */
    public Sequence query(String queryString) throws XPathException {
        Query q;
        try {
            q = QueryFactory.create(queryString);

            // all query types not implemented yet
            if (!(q.isSelectType())) {
                throw new XPathException("SPARQL query type not supported:" + q.getQueryType());
            }

            try (QueryExecution qe = QueryExecutionFactory.create(q, index.getDataset())) {

                if (q.isSelectType()) {
                    ResultSet result = qe.execSelect();
                    DocumentBuilderReceiver builder = new DocumentBuilderReceiver();
                    JenaResultSet2Sax jenaResultSet2Sax = new JenaResultSet2Sax(builder);
                    ResultSetApply.apply(result, jenaResultSet2Sax);

                    return ((Sequence) builder.getDocument());
                }
            }
        } catch (QueryException ex) { // query parsing or execution exception
            LOG.warn("QueryException: " + ex.getLocalizedMessage());
            throw new XPathException(ex);
        }

        // shouldnt come here
        return null;
    }

    private class TDBStreamListener extends AbstractStreamListener {

        private ElementImpl deferredElement;
        private final AttributesImpl deferredAttribs = new AttributesImpl();
        private SAX2Model saxHandler;

        private TDBStreamListener() {
            try {
                saxHandler = SAX2Model.create("", cacheModel);
            } catch (SAXParseException ex) {
                LOG.error(ex);
            }
        }

        public void reset(Document doc) {
            String base = "";//"resource://" + doc.getBaseURI();
            String lang = "";
            // todo? figure out URI base and lang.
            // how can we know if doc has root node yet? getFirstChild prints error messages and stacktrace when root node not yet built
//            if (doc.hasChildNodes()) {
//                ElementImpl root = (ElementImpl) doc.getFirstChild();
//                if (root != null) {
//                    base = root.getBaseURI();
//                    String langAttr = root.getAttributeNS(NamespaceSupport.XMLNS, "lang");
//                    if (langAttr != null) lang = langAttr; 
//                }
//            }
            try {
                saxHandler.initParse(base, lang);
            } catch (SAXParseException ex) {
                LOG.error(ex);
            }

            if (!cacheModel.isEmpty()) {
                LOG.warn("TDBStreamListener: Model is not empty at reset");
                cacheModel.removeAll();
            }
        }

        @Override
        public IndexWorker getWorker() {
            return TDBIndexWorker.this;
        }

        @Override
        public void startElement(Txn transaction, ElementImpl element, NodePath path) {
            if (deferredElement != null) {
                processDeferredElement();
            }
            deferredElement = element;
            super.startElement(transaction, element, path);
        }

        @Override
        public void endElement(Txn transaction, ElementImpl element, NodePath path) {
            if (deferredElement != null) {
                processDeferredElement();
            }
            try {
                saxHandler.endElement(
                        element.getNamespaceURI(),
                        element.getLocalName(),
                        element.getQName().getStringValue());
            } catch (Exception ex) {
                LOG.error(ex);
            }
            super.endElement(transaction, element, path);
        }

        @Override
        public void attribute(Txn transaction, AttrImpl attrib, NodePath path) {
            deferredAttribs.addAttribute(
                    attrib.getNamespaceURI(),
                    attrib.getLocalName(),
                    attrib.getQName().getStringValue(),
                    AttrImpl.getAttributeType(attrib.getType()),
                    attrib.getValue()
            );
            super.attribute(transaction, attrib, path);
        }

        @Override
        public void characters(Txn transaction, AbstractCharacterData text, NodePath path) {
            if (deferredElement != null) {
                processDeferredElement();
            }
            try {
                saxHandler.characters(text.getData().toCharArray(), 0, text.getLength());
            } catch (Exception ex) {
                LOG.error(ex);
            }
            super.characters(transaction, text, path);
        }

        private void processDeferredElement() {
            try {

                saxHandler.startElement(
                        deferredElement.getNamespaceURI(),
                        deferredElement.getLocalName(),
                        deferredElement.getQName().getStringValue(),
                        deferredAttribs
                );
            } catch (Exception ex) {
                LOG.error(ex);
//                deferredAttribs.clear();
            } finally {
                deferredAttribs.clear();
                deferredElement = null;
            }
        }

    }

}
