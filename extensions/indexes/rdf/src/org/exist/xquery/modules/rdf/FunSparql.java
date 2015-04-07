package org.exist.xquery.modules.rdf;

import org.apache.log4j.Logger;
import org.exist.dom.QName;
import org.exist.indexing.rdf.RDFIndex;
import org.exist.indexing.rdf.TDBIndexWorker;
import org.exist.xquery.*;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

/**
 *
 * @author Anton Olsson <abc2386@gmail.com>
 */
public class FunSparql extends BasicFunction {

    protected static Logger LOG = Logger.getLogger(FunSparql.class);

    public final static FunctionSignature signature = new FunctionSignature(
            new QName("query", SparqlModule.NAMESPACE_URI, null),
            "Query the RDF index.",
            new SequenceType[]{
                new FunctionParameterSequenceType("queryString", Type.STRING, Cardinality.EXACTLY_ONE, "SPARQL query string")
            },
            new FunctionReturnSequenceType(Type.NODE, Cardinality.EXACTLY_ONE, "Solution set of query"));

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        TDBIndexWorker worker = (TDBIndexWorker) context.getBroker().getIndexController().getWorkerByIndexId(RDFIndex.ID);

        String query = args[0].toString();

        Sequence result = worker.query(query);

        return result;
    }

    public FunSparql(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

}
