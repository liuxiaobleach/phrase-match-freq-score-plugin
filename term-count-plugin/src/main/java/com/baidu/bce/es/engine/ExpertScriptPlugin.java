package com.baidu.bce.es.engine;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScoreScript.LeafFactory;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;

/**
 * An example script plugin that adds a {@link ScriptEngine} implementing expert scoring.
 *
 * @author Xiao Liu (liuxiao14@baidu.com)
 */
public class ExpertScriptPlugin extends Plugin implements ScriptPlugin {

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new MyExpertScriptEngine();
    }

    private static class MyExpertScriptEngine implements ScriptEngine {
        @Override
        public String getType() {
            return "expert_scripts";
        }

        @Override
        public <T> T compile(String scriptName, String scriptSource,
                             ScriptContext<T> context, Map<String, String> params) {
            if (context.equals(ScoreScript.CONTEXT) == false) {
                throw new IllegalArgumentException(getType()
                        + " scripts cannot be used for context ["
                        + context.name + "]");
            }
            // we use the script "source" as the script identifier
            if ("pure_df".equals(scriptSource)) {
                ScoreScript.Factory factory = PureDfLeafFactory::new;
                return context.factoryClazz.cast(factory);
            }
            throw new IllegalArgumentException("Unknown script name "
                    + scriptSource);
        }

        @Override
        public void close() {
            // optionally close resources
        }

        private static class PureDfLeafFactory implements LeafFactory {
            private final Map<String, Object> params;
            private final SearchLookup lookup;
            private final String field;
            private final String term;
            private final SearchType type;

            private PureDfLeafFactory(
                    Map<String, Object> params, SearchLookup lookup) {
                if (params.containsKey("field") == false) {
                    throw new IllegalArgumentException("Missing parameter [field]");
                }
                if (params.containsKey("term") == false) {
                    throw new IllegalArgumentException("Missing parameter [term]");
                }
                if (params.containsKey("type") == false) {
                    type = SearchType.TERM;
                } else {
                    type = SearchType.parse(params.get("type").toString());
                }
                this.params = params;
                this.lookup = lookup;
                field = params.get("field").toString();
                term = params.get("term").toString();
                if (type == null) {
                    throw new IllegalArgumentException("Error parameter [type]");
                }
                if (term == null || term.trim().equals("")) {
                    throw new IllegalArgumentException("Empty parameter [term]");
                }
            }

            @Override
            public boolean needs_score() {
                return false;  // Return true if the script needs the score
            }

            /**
             * When use match, the text content splited by space must more than 1, or 0 will be returned.
             *
             * @param context
             * @return
             * @throws IOException
             */
            @Override
            public ScoreScript newInstance(LeafReaderContext context)
                    throws IOException {
                if (type == SearchType.TERM) {
                    PostingsEnum postings = context.reader().postings(new Term(field, term));
                    if (postings == null) {
                        /*
                         * the field and/or term don't exist in this segment,
                         * so always return 0
                         */
                        return new ScoreScript(params, lookup, context) {
                            @Override
                            public double execute() {
                                return 0.0d;
                            }
                        };
                    }
                    return new ScoreScript(params, lookup, context) {
                        int currentDocid = -1;
                        @Override
                        public void setDocument(int docid) {
                            /*
                             * advance has undefined behavior calling with
                             * a docid <= its current docid
                             */
                            if (postings.docID() < docid) {
                                try {
                                    postings.advance(docid);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }
                            currentDocid = docid;
                        }
                        @Override
                        public double execute() {
                            if (postings.docID() != currentDocid) {
                                /*
                                 * advance moved past the current doc, so this doc
                                 * has no occurrences of the term
                                 */
                                return 0.0d;
                            }
                            try {
                                return postings.freq();
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                    };
                } else if (type == SearchType.MATCH) {
                    Term[] terms = toTerms(field, term.split(" "));
                    if (terms.length < 2) {
                        return new ScoreScript(params, lookup, context) {
                            @Override
                            public double execute() {
                                return 0.0d;
                            }
                        };
                    }
                    final PostingsEnum[] postingsEnums = new PostingsEnum[terms.length];
                    for (int i = 0; i < terms.length; i++) {
                        postingsEnums[i] = context.reader().postings(terms[i], PostingsEnum.POSITIONS);
                        if (postingsEnums[i] == null) {
                            return new ScoreScript(params, lookup, context) {
                                @Override
                                public double execute() {
                                    return 0.0d;
                                }
                            };
                        }
                    }
                    return new ScoreScript(params, lookup, context) {
                        int currentDocid = -1;
                        @Override
                        public void setDocument(int docid) {
                            /*
                             * advance has undefined behavior calling with
                             * a docid <= its current docid
                             */
                            for (int i = 0; i < postingsEnums.length; i++) {
                                if (postingsEnums[i].docID() < docid) {
                                    try {
                                        postingsEnums[i].advance(docid);
                                    } catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                }
                            }
                            currentDocid = docid;
                        }
                        @Override
                        public double execute() {
                            float count = 0;
                            for (int i = 0; i < postingsEnums.length; i++) {
                                if (postingsEnums[i].docID() != currentDocid) {
                                    /*
                                     * advance moved past the current doc, so this doc
                                     * has no occurrences of the term
                                     */
                                    return count;
                                }
                            }
                            if (postingsEnums.length < 1) {
                                return count;
                            }
                            try {
                                /**
                                 * indexList store current position of each term in current doc.
                                 */
                                int[] freqList = new int[postingsEnums.length];
                                int[] indexList = new int[postingsEnums.length];
                                for (int i = 0; i < postingsEnums.length; i++) {
                                    freqList[i] = postingsEnums[i].freq();
                                    indexList[i] = postingsEnums[i].nextPosition();
                                    if (indexList[i] < 0 || freqList[i] <= 0) {
                                        return 0.0f;
                                    }
                                }
                                /**
                                 * O(n) algorithm, example: abc,
                                 * a(position): 1 4 7
                                 * b(position): 2 5 8
                                 * c(position): 3 6 9
                                 * return 3.
                                 * with abc in continuous positions: 1 2 3, 4 5 6, 7 8 9.
                                 */
                                while(true) {
                                    for (int j = 0; j < (postingsEnums.length - 1); j++) {
                                        int distance = (indexList[j + 1] - indexList[j]);
                                        if (distance < 1) {
                                            int tmp = postingsEnums[j + 1].nextPosition();
                                            freqList[j + 1]--;
                                            if (freqList[j + 1] <= 0) {
                                                return count;
                                            }
                                            indexList[j + 1] = tmp;
                                            j--;
                                            continue;
                                        } else if (distance > 1) {
                                            int tmp = postingsEnums[j].nextPosition();
                                            freqList[j]--;
                                            if (freqList[j] <= 0) {
                                                return count;
                                            }
                                            indexList[j] = tmp;
                                            j--;
                                            continue;
                                        }
                                        if (j == (postingsEnums.length - 2)) {
                                            count++;
                                            /**
                                             * start a new loop from first term keyword.
                                             */
                                            int tmp = postingsEnums[0].nextPosition();
                                            freqList[0]--;
                                            if (freqList[0] <= 0) {
                                                return count;
                                            }
                                            indexList[0] = tmp;
                                        }
                                    }
                                }
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                    };
                } else {
                    return new ScoreScript(params, lookup, context) {
                        @Override
                        public void setDocument(int docid) {
                        }
                        @Override
                        public double execute() {
                            return 0.0d;
                        }
                    };
                }
            }
        }
    }
    // end::expert_engine

    private static Term[] toTerms(String field, String[] termStrings) {
        Term[] terms = new Term[termStrings.length];
        for (int i = 0; i < terms.length; ++i) {
            terms[i] = new Term(field, termStrings[i]);
        }
        return terms;
    }
}
