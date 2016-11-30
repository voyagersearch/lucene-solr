
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

package org.apache.solr.update;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.LegacyDoubleField;
import org.apache.lucene.document.LegacyFloatField;
import org.apache.lucene.document.LegacyIntField;
import org.apache.lucene.document.LegacyLongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.FieldInfo;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.TestRTGBase;
import org.apache.solr.update.processor.AtomicUpdateDocumentMerger;
import org.apache.solr.util.RefCounted;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the in-place updates (docValues updates) for a standalone Solr instance.
 */
public class TestInPlaceUpdatesStandalone extends TestRTGBase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml", "schema-inplace-updates.xml");

    // sanity check that autocommits are disabled
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxDocs);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxDocs);

    // validate that the schema was not changed to an unexpected state
    IndexSchema schema = h.getCore().getLatestSchema();
    assertTrue(schema.getFieldOrNull("_version_").hasDocValues() && !schema.getFieldOrNull("_version_").indexed()
        && !schema.getFieldOrNull("_version_").stored());
    assertTrue(schema.getFieldOrNull("inplace_updatable_float").hasDocValues() && !schema.getFieldOrNull("inplace_updatable_float").indexed()
        && !schema.getFieldOrNull("inplace_updatable_float").stored());
  }

  @Before
  public void before() {
    clearIndex();
    assertU(commit("softCommit", "false"));
  }

  @Test
  public void testUpdatingDocValues() throws Exception {
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first"), null);
    long version2 = addAndGetVersion(sdoc("id", "2", "title_s", "second"), null);
    long version3 = addAndGetVersion(sdoc("id", "3", "title_s", "third"), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*"), "//*[@numFound='3']");

    // the reason we're fetching these docids is to validate that the subsequent updates 
    // are done in place and don't cause the docids to change
    int docid1 = Integer.parseInt(getFieldValueIndex("1", "[docid]"));
    int docid2 = Integer.parseInt(getFieldValueIndex("2", "[docid]"));
    int docid3 = Integer.parseInt(getFieldValueIndex("3", "[docid]"));

    // Check docValues were "set"
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 200));
    version2 = addAndAssertVersion(version2, "id", "2", "inplace_updatable_float", map("set", 300));
    version3 = addAndAssertVersion(version3, "id", "3", "inplace_updatable_float", map("set", 100));
    assertU(commit("softCommit", "false"));

    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]"),
        "//*[@numFound='3']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='200.0']",
        "//result/doc[2]/float[@name='inplace_updatable_float'][.='300.0']",
        "//result/doc[3]/float[@name='inplace_updatable_float'][.='100.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
        "//result/doc[2]/long[@name='_version_'][.='"+version2+"']",
        "//result/doc[3]/long[@name='_version_'][.='"+version3+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']",
        "//result/doc[2]/int[@name='[docid]'][.='"+docid2+"']",
        "//result/doc[3]/int[@name='[docid]'][.='"+docid3+"']"
        );

    // Check docValues are "inc"ed
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", 1));
    version2 = addAndAssertVersion(version2, "id", "2", "inplace_updatable_float", map("inc", -2));
    version3 = addAndAssertVersion(version3, "id", "3", "inplace_updatable_float", map("inc", 3));
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]"),
        "//*[@numFound='3']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='201.0']",
        "//result/doc[2]/float[@name='inplace_updatable_float'][.='298.0']",
        "//result/doc[3]/float[@name='inplace_updatable_float'][.='103.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
        "//result/doc[2]/long[@name='_version_'][.='"+version2+"']",
        "//result/doc[3]/long[@name='_version_'][.='"+version3+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']",
        "//result/doc[2]/int[@name='[docid]'][.='"+docid2+"']",
        "//result/doc[3]/int[@name='[docid]'][.='"+docid3+"']"
        );

    // Check back to back "inc"s are working (off the transaction log)
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", 1));
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", 2)); // new value should be 204
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:1", "fl", "*,[docid]"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='204.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']");

    // Now let the document be atomically updated (non-inplace), ensure the old docvalue is part of new doc
    version1 = addAndAssertVersion(version1, "id", "1", "title_s", map("set", "new first"));
    assertU(commit("softCommit", "false"));
    int newDocid1 = Integer.parseInt(getFieldValueIndex("1", "[docid]"));
    assertTrue(newDocid1 != docid1);
    docid1 = newDocid1;

    assertQ(req("q", "id:1"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='204.0']",
        "//result/doc[1]/str[@name='title_s'][.='new first']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']");

    // Check if atomic update with "inc" to a docValue works
    version2 = addAndAssertVersion(version2, "id", "2", "title_s", map("set", "new second"), "inplace_updatable_float", map("inc", 2));
    assertU(commit("softCommit", "false"));
    int newDocid2 = Integer.parseInt(getFieldValueIndex("2", "[docid]"));
    assertTrue(newDocid2 != docid2);
    docid2 = newDocid2;

    assertQ(req("q", "id:2"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='300.0']",
        "//result/doc[1]/str[@name='title_s'][.='new second']",
        "//result/doc[1]/long[@name='_version_'][.='"+version2+"']");

    // Check if docvalue "inc" update works for a newly created document, which is not yet committed
    // Case1: docvalue was supplied during add of new document
    long version4 = addAndGetVersion(sdoc("id", "4", "title_s", "fourth", "inplace_updatable_float", "400"), params());
    version4 = addAndAssertVersion(version4, "id", "4", "inplace_updatable_float", map("inc", 1));
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:4"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='401.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version4+"']");

    // Check if docvalue "inc" update works for a newly created document, which is not yet committed
    // Case2: docvalue was not supplied during add of new document, should assume default
    long version5 = addAndGetVersion(sdoc("id", "5", "title_s", "fifth"), params());
    version5 = addAndAssertVersion(version5, "id", "5", "inplace_updatable_float", map("inc", 1));
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:5"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='1.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version5+"']");

    // Check if docvalue "set" update works for a newly created document, which is not yet committed
    long version6 = addAndGetVersion(sdoc("id", "6", "title_s", "sixth"), params());
    version6 = addAndAssertVersion(version6, "id", "6", "inplace_updatable_float", map("set", 600));
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:6"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='600.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version6+"']");

    // Check optimistic concurrency works
    long v20 = addAndGetVersion(sdoc("id", "20", "title_s","first", "inplace_updatable_float", 100), params());    
    SolrException exception = expectThrows(SolrException.class, () -> {
      addAndGetVersion(sdoc("id","20", "_version_", -1, "inplace_updatable_float", map("inc", 1)), null);
    });
    assertTrue(exception.getMessage().contains("conflict"));



    long oldV20 = v20;
    v20 = addAndAssertVersion(v20, "id","20", "_version_", v20, "inplace_updatable_float", map("inc", 1));
    exception = expectThrows(SolrException.class, () -> {
      addAndGetVersion(sdoc("id","20", "_version_", oldV20, "inplace_updatable_float", map("inc", 1)), null);
    });
    assertTrue(exception.getMessage().contains("conflict"));

    v20 = addAndAssertVersion(v20, "id","20", "_version_", v20, "inplace_updatable_float", map("inc", 1));
    // RTG before a commit
    assertJQ(req("qt","/get", "id","20", "fl","id,inplace_updatable_float,_version_"),
        "=={'doc':{'id':'20', 'inplace_updatable_float':" + 102.0 + ",'_version_':" + v20 + "}}");
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:20"), 
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='102.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+v20+"']");

    // Check if updated DVs can be used for search
    assertQ(req("q", "inplace_updatable_float:102"), 
        "//result/doc[1]/str[@name='id'][.='20']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='102.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+v20+"']");

    // Check if updated DVs can be used for sorting
    assertQ(req("q", "*:*", "sort", "inplace_updatable_float asc"), 
        "//result/doc[4]/str[@name='id'][.='1']",
        "//result/doc[4]/float[@name='inplace_updatable_float'][.='204.0']",

        "//result/doc[5]/str[@name='id'][.='2']",
        "//result/doc[5]/float[@name='inplace_updatable_float'][.='300.0']",

        "//result/doc[3]/str[@name='id'][.='3']",
        "//result/doc[3]/float[@name='inplace_updatable_float'][.='103.0']",

        "//result/doc[6]/str[@name='id'][.='4']",
        "//result/doc[6]/float[@name='inplace_updatable_float'][.='401.0']",

        "//result/doc[1]/str[@name='id'][.='5']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='1.0']",

        "//result/doc[7]/str[@name='id'][.='6']",
        "//result/doc[7]/float[@name='inplace_updatable_float'][.='600.0']",

        "//result/doc[2]/str[@name='id'][.='20']",
        "//result/doc[2]/float[@name='inplace_updatable_float'][.='102.0']");
  }

  @Test
  public void testUpdateTwoDifferentFields() throws Exception {
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first"), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*"), "//*[@numFound='1']");

    int docid1 = Integer.parseInt(getFieldValueIndex("1", "[docid]"));

    // Check docValues were "set"
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 200));
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_int", map("set", 10));
    assertU(commit("softCommit", "false"));

    assertU(commit("softCommit", "false"));

    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]"),
        "//*[@numFound='1']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='200.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']"
        );

    // two different update commands, updating each of the fields separately
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_int", map("inc", 1));
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", 1));
    // same update command, updating both the fields together
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_int", map("inc", 1),
        "inplace_updatable_float", map("inc", 1));

    if (random().nextBoolean()) {
      assertU(commit("softCommit", "false"));
      assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]"),
          "//*[@numFound='1']",
          "//result/doc[1]/float[@name='inplace_updatable_float'][.='202.0']",
          "//result/doc[1]/int[@name='inplace_updatable_int'][.='12']",
          "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
          "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']"
          );
    } 

    // RTG
    assertJQ(req("qt","/get", "id","1", "fl","id,inplace_updatable_float,inplace_updatable_int"),
        "=={'doc':{'id':'1', 'inplace_updatable_float':" + 202.0 + ",'inplace_updatable_int':" + 12 + "}}");

  }

  @Test
  public void testDVUpdatesWithDBQofUpdatedValue() throws Exception {
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first", "inplace_updatable_float", "0"), null);
    assertU(commit());

    // in-place update
    addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 100), "_version_", version1);

    // DBQ where q=inplace_updatable_float:100
    assertU(delQ("inplace_updatable_float:100"));

    assertU(commit());

    assertQ(req("q", "*:*"), "//*[@numFound='0']");
  }

  @Test
  public void testDVUpdatesWithDelete() throws Exception {
    long version1 = 0;

    for (boolean postAddCommit : Arrays.asList(true, false)) {
      for (boolean delById : Arrays.asList(true, false)) {
        for (boolean postDelCommit : Arrays.asList(true, false)) {
          addAndGetVersion(sdoc("id", "1", "title_s", "first"), params());
          if (postAddCommit) assertU(commit());
          assertU(delById ? delI("1") : delQ("id:1"));
          if (postDelCommit) assertU(commit());
          version1 = addAndGetVersion(sdoc("id", "1", "inplace_updatable_float", map("set", 200)), params());
          // assert current doc#1 doesn't have old value of "title_s"
          assertU(commit());
          assertQ(req("q", "title_s:first", "sort", "id asc", "fl", "*,[docid]"),
              "//*[@numFound='0']");
        }
      }
    }

    // Update to recently deleted (or non-existent) document with a "set" on updateable 
    // field should succeed, since it is executed internally as a full update
    // because AUDM.doInPlaceUpdateMerge() returns false
    assertU(random().nextBoolean()? delI("1"): delQ("id:1"));
    if (random().nextBoolean()) assertU(commit());
    addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 200));
    assertU(commit());
    assertQ(req("q", "id:1", "sort", "id asc", "fl", "*"),
        "//*[@numFound='1']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='200.0']");

    // Another "set" on the same field should be an in-place update 
    int docid1 = getDocId("1");
    addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 300));
    assertU(commit());
    assertQ(req("q", "id:1", "fl", "*,[docid]"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='300.0']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']");
  }

  public static long addAndAssertVersion(long expectedCurrentVersion, Object... fields) throws Exception {
    assert 0 < expectedCurrentVersion;
    long currentVersion = addAndGetVersion(sdoc(fields), null);
    assertTrue(currentVersion > expectedCurrentVersion);
    return currentVersion;
  }

  private int getDocId(String id) throws NumberFormatException, Exception {
    return Integer.parseInt(getFieldValueIndex(id, "[docid]"));
  }

  @Test
  public void testUpdateOfNonExistentDVsShouldNotFail() throws Exception {
    // schema sanity check: assert that the nonexistent_field_i_dvo doesn't exist already
    FieldInfo fi;
    RefCounted<SolrIndexSearcher> holder = h.getCore().getSearcher();
    try {
      fi = holder.get().getSlowAtomicReader().getFieldInfos().fieldInfo("nonexistent_field_i_dvo");
    } finally {
      holder.decref();
    }
    assertNull(fi);

    // Partial update
    addAndGetVersion(sdoc("id", "0", "nonexistent_field_i_dvo", map("set", "42")), null);

    addAndGetVersion(sdoc("id", "1"), null);
    addAndGetVersion(sdoc("id", "1", "nonexistent_field_i_dvo", map("inc", "1")), null);
    addAndGetVersion(sdoc("id", "1", "nonexistent_field_i_dvo", map("inc", "1")), null);

    assertU(commit());

    assertQ(req("q", "*:*"), "//*[@numFound='2']");    
    assertQ(req("q", "nonexistent_field_i_dvo:42"), "//*[@numFound='1']");    
    assertQ(req("q", "nonexistent_field_i_dvo:2"), "//*[@numFound='1']");    
  }

  @Test
  public void testOnlyPartialUpdatesBetweenCommits() throws Exception {
    // Full updates
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first", "val1_i_dvo", "1", "val2_l_dvo", "1"), params());
    long version2 = addAndGetVersion(sdoc("id", "2", "title_s", "second", "val1_i_dvo", "2", "val2_l_dvo", "2"), params());
    long version3 = addAndGetVersion(sdoc("id", "3", "title_s", "third", "val1_i_dvo", "3", "val2_l_dvo", "3"), params());
    assertU(commit("softCommit", "false"));

    assertQ(req("q", "*:*", "fl", "*,[docid]"), "//*[@numFound='3']");

    int docid1 = Integer.parseInt(getFieldValueIndex("1", "[docid]"));
    int docid2 = Integer.parseInt(getFieldValueIndex("2", "[docid]"));
    int docid3 = Integer.parseInt(getFieldValueIndex("3", "[docid]"));

    int numPartialUpdates = 1 + random().nextInt(5000);
    for (int i=0; i<numPartialUpdates; i++) {
      version1 = addAndAssertVersion(version1, "id", "1", "val1_i_dvo", map("set", i));
      version2 = addAndAssertVersion(version2, "id", "2", "val1_i_dvo", map("inc", 1));
      version3 = addAndAssertVersion(version3, "id", "3", "val1_i_dvo", map("set", i));

      version1 = addAndAssertVersion(version1, "id", "1", "val2_l_dvo", map("set", i));
      version2 = addAndAssertVersion(version2, "id", "2", "val2_l_dvo", map("inc", 1));
      version3 = addAndAssertVersion(version3, "id", "3", "val2_l_dvo", map("set", i));
    }
    assertU(commit("softCommit", "true"));

    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]"),
        "//*[@numFound='3']",
        "//result/doc[1]/int[@name='val1_i_dvo'][.='"+(numPartialUpdates-1)+"']",
        "//result/doc[2]/int[@name='val1_i_dvo'][.='"+(numPartialUpdates+2)+"']",
        "//result/doc[3]/int[@name='val1_i_dvo'][.='"+(numPartialUpdates-1)+"']",
        "//result/doc[1]/long[@name='val2_l_dvo'][.='"+(numPartialUpdates-1)+"']",
        "//result/doc[2]/long[@name='val2_l_dvo'][.='"+(numPartialUpdates+2)+"']",
        "//result/doc[3]/long[@name='val2_l_dvo'][.='"+(numPartialUpdates-1)+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']",
        "//result/doc[2]/int[@name='[docid]'][.='"+docid2+"']",
        "//result/doc[3]/int[@name='[docid]'][.='"+docid3+"']",
        "//result/doc[1]/long[@name='_version_'][.='" + version1 + "']",
        "//result/doc[2]/long[@name='_version_'][.='" + version2 + "']",
        "//result/doc[3]/long[@name='_version_'][.='" + version3 + "']"
        );
  }

  private String getFieldValueRTG(String id, String field) throws Exception {
    try (SolrQueryRequest request = req("id",id, "fl", field)) { //(SolrQueryRequest request = req("q", "id:"+id, "fl", field)) {
      SolrQueryResponse resp = h.queryAndResponse("/get", request);
      Object val = ((SolrDocument)resp.getValues().get("doc")).getFieldValue(field);
      if (val instanceof StoredField) {
        return ((StoredField)val).stringValue();
      }
      if (val instanceof NumericDocValuesField) {
        return ((NumericDocValuesField)val).numericValue().toString(); // TODO: somehow .stringValue() isn't working
      }
      if (val instanceof LegacyIntField) {
        return ((LegacyIntField)val).numericValue().toString();
      }
      if (val instanceof LegacyLongField) {
        return ((LegacyLongField)val).numericValue().toString();
      }
      if (val instanceof LegacyFloatField) {
        return ((LegacyFloatField)val).numericValue().toString();
      }
      if (val instanceof LegacyDoubleField) {
        return ((LegacyDoubleField)val).numericValue().toString();
      }
      
      return val.toString();
    }
  }

  private String getFieldValueIndex(String id, String field) throws Exception {
    try (SolrQueryRequest request = req("q", "id:"+id, "fl", field)) {
      SolrQueryResponse resp = h.queryAndResponse(null, request);
      return ((ResultContext)resp.getResponse()).getProcessedDocuments().next().get(field).toString();
    }
  }

  /**
   * Useful to store the state of an expected document into an in-memory model
   * representing the index.
   */
  private static class DocInfo {
    long version;
    long value;

    public DocInfo(long version, long val) {
      this.version = version;
      this.value = val;
    }

    @Override
    public String toString() {
      return "["+version+", "+value+"]";
    }
  }

  @Test
  /**
   * Test involving replaying of some hardcoded sets of operations that involve in-place updates, commits, full
   * document updates. 
   * TODO: Add a fully automated version of this test, where such sets of operations can be
   * generated on the fly.
   */
  public void testReplay() throws Exception {
    checkReplay("val2_l_dvo",
        //
        sdoc("id", "0", "val2_l_dvo", "3000000000"),
        sdoc("id", "0", "val2_l_dvo", map("inc", "3")),
        HARDCOMMIT,
        sdoc("id", "0", "val2_l_dvo", map("inc", "5")),
        sdoc("id", "1", "val2_l_dvo", "2000000000"),
        sdoc("id", "1", "val2_l_dvo", map("set", "2000000002")),
        sdoc("id", "1", "val2_l_dvo", map("set", "3000000000")),
        sdoc("id", "0", "val2_l_dvo", map("inc", "7")),
        sdoc("id", "1", "val2_l_dvo", map("set", "7000000000")),
        sdoc("id", "0", "val2_l_dvo", map("inc", "11")),
        sdoc("id", "2", "val2_l_dvo", "2000000000"),
        HARDCOMMIT,
        sdoc("id", "2", "val2_l_dvo", map("set", "3000000000")),
        HARDCOMMIT);

    clearIndex();
    assertU(commit("softCommit", "false"));

    // test various set operations
    checkReplay("val2_l_dvo",
        //
        sdoc("id", "0", "val2_l_dvo", "3000000000"),
        sdoc("id", "0", "val2_l_dvo", map("set", "3000000003")),
        HARDCOMMIT,
        sdoc("id", "0", "val2_l_dvo", map("set", "3000000008")),
        sdoc("id", "1", "val2_l_dvo", "2000000000"),
        sdoc("id", "1", "val2_l_dvo", map("inc", "2")),
        sdoc("id", "1", "val2_l_dvo", "3000000000"),
        sdoc("id", "0", "val2_l_dvo", map("set", "3000000015")),
        sdoc("id", "1", "val2_l_dvo", "7000000000"),
        sdoc("id", "0", "val2_l_dvo", map("set", "3000000026")),
        sdoc("id", "2", "val2_l_dvo", "2000000000"),
        HARDCOMMIT,
        sdoc("id", "2", "val2_l_dvo", "3000000000"),
        HARDCOMMIT);

    clearIndex();
    assertU(commit("softCommit", "false"));

    // test various inc operations 
    checkReplay("val2_l_dvo",
        sdoc("id", "0", "val2_l_dvo", "3000000000"),
        sdoc("id", "0", "val2_l_dvo", map("inc", "3")),
        HARDCOMMIT,
        sdoc("id", "0", "val2_l_dvo", map("inc", "5")),
        sdoc("id", "1", "val2_l_dvo", "2000000000"),
        sdoc("id", "1", "val2_l_dvo", map("inc", "2")),
        sdoc("id", "1", "val2_l_dvo", "3000000000"),
        sdoc("id", "0", "val2_l_dvo", map("inc", "7")),
        sdoc("id", "1", "val2_l_dvo", "7000000000"),
        sdoc("id", "0", "val2_l_dvo", map("inc", "11")),
        sdoc("id", "2", "val2_l_dvo", "2000000000"),
        HARDCOMMIT,
        sdoc("id", "2", "val2_l_dvo", "3000000000"),
        HARDCOMMIT);

    clearIndex();
    assertU(commit("softCommit", "false"));

    // test various set operations
    checkReplay("val2_l_dvo",
        //
        // use 'set' for all non-id=0 updates *AFTER* the doc is initially added
        //
        sdoc("id", "0", "val2_l_dvo", "3000000000"),
        sdoc("id", "0", "val2_l_dvo", map("set", "3000000003")),
        HARDCOMMIT,
        sdoc("id", "0", "val2_l_dvo", map("set", "3000000008")),
        sdoc("id", "1", "val2_l_dvo", "2000000000"),
        sdoc("id", "1", "val2_l_dvo", map("set", "2000000002")),
        sdoc("id", "1", "val2_l_dvo", map("set", "3000000000")),
        sdoc("id", "0", "val2_l_dvo", map("set", "3000000015")),
        sdoc("id", "1", "val2_l_dvo", map("set", "7000000000")),
        sdoc("id", "0", "val2_l_dvo", map("set", "3000000026")),
        sdoc("id", "2", "val2_l_dvo", "2000000000"),
        HARDCOMMIT,
        sdoc("id", "2", "val2_l_dvo", map("set", "3000000000")),
        HARDCOMMIT
        );
  }

  public Object SOFTCOMMIT = new Object();
  public Object HARDCOMMIT = new Object();

  /**
   * Note: 'id' must be uniqueKey field, and must be an int.
   *
   * @param valField a (long) field name that uses docvalues to check for in place updates (which may be atomic via maps containing 'inc')
   * @param docs It can either be a SolrInputDocument containing a partial or full
   *              update command with document or HARDCOMMIT or SOFTCOMMIT objects.
   */
  public void checkReplay(final String valField, Object... docs) throws Exception {

    HashMap<Integer, DocInfo> model = new LinkedHashMap<>();
    HashMap<Integer, DocInfo> committedModel = new LinkedHashMap<>();

    for (Object doc: docs) {
      if (doc == SOFTCOMMIT) {
        assertU(commit("softCommit", "true"));
        committedModel = new LinkedHashMap(model);
      } else if (doc == HARDCOMMIT) {
        assertU(commit("softCommit", "false"));
        committedModel = new LinkedHashMap(model);
      } else {
        SolrInputDocument sdoc = (SolrInputDocument) doc;
        long version = addAndGetVersion(sdoc, null);
        int id = Integer.parseInt(sdoc.getFieldValue("id").toString());
        Object val = sdoc.getFieldValue(valField);
        if (val instanceof Map) {
          Map<String,String> atomicUpdate = (Map) val;
          assertEquals(sdoc.toString(), 1, atomicUpdate.size());
          if (atomicUpdate.containsKey("inc")) {
            model.put(id, new DocInfo(version,
                model.get(id).value + Long.parseLong(atomicUpdate.get("inc"))));
          } else if (atomicUpdate.containsKey("set")) {
            model.put(id, new DocInfo(version, Long.parseLong(atomicUpdate.get("set"))));
          } else {
            fail("wtf update is this? ... " + doc);
          }
        } else if (null == val) {
          // our model doesn't change
          model.put(id, new DocInfo(version, model.get(id).value));
        } else {
          assertEquals("wtf is value?", String.class, val.getClass());
          model.put(id, new DocInfo(version, Long.parseLong(val.toString())));
        }
      }

      // after every op, check the model(s)
      // RTG to check the values for every id against the model
      for (int id: model.keySet()) {
        assertEquals(String.valueOf(model.get(id).value), getFieldValueRTG(String.valueOf(id), valField));
      }

      for (Map.Entry<Integer, DocInfo> entry : committedModel.entrySet()) {
        Integer id = entry.getKey();
        long expected = entry.getValue().value;
        assertQ(req("q", "id:" + id),
            "//*[@numFound='1']",
            "//result/doc[1]/long[@name='"+valField+"'][.='"+expected+"']"
            );
      }
    }
  }

  @Test
  public void testMixedInPlaceAndNonInPlaceAtomicUpdates() throws Exception {
    long version1 = addAndGetVersion(sdoc("id", "1", "inplace_updatable_float", "100", "stored_i", "100"), params());

    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", "1"), "stored_i", map("inc", "1"));
    assertEquals("101", getFieldValueRTG("1", "stored_i"));
    assertEquals(""+Float.floatToIntBits(101.0f), 
        getFieldValueRTG("1", "inplace_updatable_float")); // TODO: RTG for float NDVs return the raw long values for uncommitted docs

    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", "1"));
    assertEquals("101", getFieldValueRTG("1", "stored_i"));
    assertEquals(""+Float.floatToIntBits(102.0f), 
        getFieldValueRTG("1", "inplace_updatable_float")); // TODO: RTG for float NDVs return the raw long values for uncommitted docs

    version1 = addAndAssertVersion(version1, "id", "1", "stored_i", map("inc", "1"));
    assertEquals("102", getFieldValueRTG("1", "stored_i"));
    assertEquals(""+Float.floatToIntBits(102.0f), 
        getFieldValueRTG("1", "inplace_updatable_float")); // TODO: RTG for float NDVs return the raw long values for uncommitted docs

    boolean postCommit = random().nextBoolean();
    if (postCommit) {
      assertU(commit("softCommit", "false"));
      assertQ(req("q", "*:*", "sort", "id asc", "fl", "*"),
          "//*[@numFound='1']",
          "//result/doc[1]/float[@name='inplace_updatable_float'][.='102.0']",
          "//result/doc[1]/int[@name='stored_i'][.='102']",
          "//result/doc[1]/long[@name='_version_'][.='" + version1 + "']"
          );
    }

    // RTG
    assertEquals("102", getFieldValueRTG("1", "stored_i"));
    if (postCommit)
      assertEquals("102.0", getFieldValueRTG("1", "inplace_updatable_float"));
    else 
      assertEquals(""+Float.floatToIntBits(102.0f), 
          getFieldValueRTG("1", "inplace_updatable_float")); // TODO: RTG for float NDVs return the raw long values for uncommitted docs (must be some bug)
  }

  @Test
  public void testIsInPlaceUpdate() throws Exception {
    Set<String> inPlaceUpdatedFields = new HashSet<String>();

    // In-place updates:
    inPlaceUpdatedFields = AtomicUpdateDocumentMerger.isInPlaceUpdate(
        UpdateLogTest.getAddUpdate(null, sdoc("id", "1", "_version_", 42L, "inplace_updatable_float", map("set", 10))));
    assertTrue(inPlaceUpdatedFields.contains("inplace_updatable_float"));

    inPlaceUpdatedFields.clear();
    inPlaceUpdatedFields = AtomicUpdateDocumentMerger.isInPlaceUpdate(
        UpdateLogTest.getAddUpdate(null, sdoc("id", "1", "_version_", 42L, "inplace_updatable_float", map("inc", 10))));
    assertTrue(inPlaceUpdatedFields.contains("inplace_updatable_float"));

    inPlaceUpdatedFields.clear();
    inPlaceUpdatedFields = AtomicUpdateDocumentMerger.isInPlaceUpdate(
        UpdateLogTest.getAddUpdate(null, sdoc("id", "1", "_version_", 42L, "inplace_updatable_int", map("set", 10))));
    assertTrue(inPlaceUpdatedFields.contains("inplace_updatable_int"));

    // Non in-place updates
    inPlaceUpdatedFields.clear();
    addAndGetVersion(sdoc("id", "1", "stored_i", "0"), params()); // setting up the dv
    assertTrue("stored field updated", AtomicUpdateDocumentMerger.isInPlaceUpdate(
        UpdateLogTest.getAddUpdate(null, sdoc("id", "1", "_version_", 42L, "stored_i", map("inc", 1)))).isEmpty());

    assertTrue("No map means full document update", AtomicUpdateDocumentMerger.isInPlaceUpdate(
        UpdateLogTest.getAddUpdate(null, sdoc("id", "1", "_version_", 42L, "inplace_updatable_int", "100"))).isEmpty());

    assertTrue("non existent dynamic dv field updated first time",
        AtomicUpdateDocumentMerger.isInPlaceUpdate(
            UpdateLogTest.getAddUpdate(null, sdoc("id", "1", "_version_", 42L, "new_updateable_int_i_dvo", map("set", 10)))).isEmpty());

    // After adding a full document with the dynamic dv field, in-place update should work
    addAndGetVersion(sdoc("id", "2", "new_updateable_int_i_dvo", "0"), params()); // setting up the dv
    if (random().nextBoolean())
      assertU(commit("softCommit", "false"));
    inPlaceUpdatedFields.clear();
    inPlaceUpdatedFields = AtomicUpdateDocumentMerger.isInPlaceUpdate(
        UpdateLogTest.getAddUpdate(null, sdoc("id", "2", "_version_", 42L, "new_updateable_int_i_dvo", map("set", 10))));
    assertTrue(inPlaceUpdatedFields.contains("new_updateable_int_i_dvo"));

    // If a supported dv field has a copyField target which is supported, it should be an in-place update
    inPlaceUpdatedFields = AtomicUpdateDocumentMerger.isInPlaceUpdate(
        UpdateLogTest.getAddUpdate(null, sdoc("id", "1", "_version_", 42L, "copyfield_updateable_src", map("set", 10))));
    assertTrue(inPlaceUpdatedFields.contains("copyfield_updateable_src"));

    // If a supported dv field has a copyField target which is not supported, it should not be an in-place update
    inPlaceUpdatedFields = AtomicUpdateDocumentMerger.isInPlaceUpdate(
        UpdateLogTest.getAddUpdate(null, sdoc("id", "1", "_version_", 42L, "copyfield_not_updateable_src", map("set", 10))));
    assertTrue(inPlaceUpdatedFields.isEmpty());
  }

  @Test
  /**
   *  Test the @see {@link AtomicUpdateDocumentMerger#doInPlaceUpdateMerge(AddUpdateCommand,Set<String>)} 
   *  method is working fine
   */
  public void testDoInPlaceUpdateMerge() throws Exception {
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first"), null);
    long version2 = addAndGetVersion(sdoc("id", "2", "title_s", "second"), null);
    long version3 = addAndGetVersion(sdoc("id", "3", "title_s", "third"), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*"), "//*[@numFound='3']");

    // Adding a few in-place updates
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 200));

    // Test the AUDM.doInPlaceUpdateMerge() method is working fine
    AddUpdateCommand cmd = UpdateLogTest.getAddUpdate(null, sdoc("id", "1", "_version_", 42L, "inplace_updatable_float", map("inc", 10)));
    SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params());
    AtomicUpdateDocumentMerger docMerger = new AtomicUpdateDocumentMerger(req);
    boolean done = docMerger.doInPlaceUpdateMerge(cmd, AtomicUpdateDocumentMerger.isInPlaceUpdate(cmd));
    assertTrue(done);
    assertEquals(42L, cmd.getSolrInputDocument().getFieldValue("_version_"));
    assertEquals(42L, cmd.getSolrInputDocument().getFieldValue("_version_"));
    assertEquals(210f, cmd.getSolrInputDocument().getFieldValue("inplace_updatable_float"));
    assertFalse(cmd.getSolrInputDocument().containsKey("title_s")); // in-place merged doc shouldn't have non-inplace fields from the index/tlog
    assertEquals(version1, cmd.prevVersion);

    // do a commit, and the same results should be repeated
    assertU(commit("softCommit", "false"));

    cmd = UpdateLogTest.getAddUpdate(null, sdoc("id", "1", "_version_", 42L, "inplace_updatable_float", map("inc", 10)));
    done = docMerger.doInPlaceUpdateMerge(cmd, AtomicUpdateDocumentMerger.isInPlaceUpdate(cmd));
    assertTrue(done);
    assertEquals(42L, cmd.getSolrInputDocument().getFieldValue("_version_"));
    assertEquals(42L, cmd.getSolrInputDocument().getFieldValue("_version_"));
    assertEquals(210f, cmd.getSolrInputDocument().getFieldValue("inplace_updatable_float"));
    assertFalse(cmd.getSolrInputDocument().containsKey("title_s")); // in-place merged doc shouldn't have non-inplace fields from the index/tlog
    assertEquals(version1, cmd.prevVersion);
  }
}
