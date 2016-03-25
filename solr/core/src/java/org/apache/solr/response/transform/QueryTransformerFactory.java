/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.response.transform;

import java.io.IOException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SyntaxError;

import com.natelenergy.drop3.solr.SolrClientUtil;


public class QueryTransformerFactory extends TransformerFactory
{ 
  public static String getString(String k, SolrDocument doc) {
    Object v = doc.getFirstValue(k);
    if(v != null) {
      if(v instanceof IndexableField) {
        return ((IndexableField)v).stringValue();
      }
      return v.toString();
    }
    return null;
  }
  
  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {
    
    Query filter = null;
    String fq = params.get("fq");
    if(fq!=null) {
      QParserPlugin qpp = req.getCore().getQueryPlugin(QParserPlugin.DEFAULT_QTYPE);
      
      QParser pp = qpp.createParser(fq, null, params, req);
      try {
        filter = pp.getQuery();
      } 
      catch (SyntaxError e) {
        throw new RuntimeException(e);
      }
    }
    
    String joinField = params.get("j");
    if(joinField ==null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Missing param 'j' for join field");
    }
    
    String subfl = params.get("sub.fl");
    if(subfl==null) {
      subfl = req.getParams().get("sub.fl");
    }
    SolrReturnFields rf = new SolrReturnFields(subfl, req);
    return new SubQueryAugmenter(field, joinField, filter, rf);
  }
}

class SubQueryAugmenter extends DocTransformer
{
  final Query filter;
  final String name;
  final String joinField;
  final SolrReturnFields rf;
  
  public SubQueryAugmenter(String name, String joinField, Query filter, SolrReturnFields rf) {
    this.name = name;
    this.joinField = joinField;
    this.rf = rf;
    this.filter = filter;
  }

  @Override
  public String[] getExtraRequestFields() {
    return new String[] {"id"};
  }
  
  @Override
  public String getName() {
    return "SubQueryAugmenter";
  }

  @Override
  public void transform(SolrDocument doc, int docid, float score) throws IOException {
    String id = SolrClientUtil.getString("id", doc);
    
    if(context.getQuery()==null || id==null) {
      return; // this happens for the sub query
    }

    // TODO, use QParser so we pick the right kind of query!
    TermQuery q = new TermQuery(new Term(joinField, id));
    DocList docs = context.getSearcher().getDocList(q, filter, null, 0, 10);
    if(docs!=null) {

      BasicResultContext rc = new BasicResultContext(docs, rf, context.getSearcher(), q, null);

      doc.setField(name, rc);
    }
  } 
}
