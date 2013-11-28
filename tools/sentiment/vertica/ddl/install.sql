-- Installation script: defined the shared library and the appropriate entry points

select version();

\set libfile '\''`pwd`'/lib/TwitterSentiment.so\'';

CREATE LIBRARY TwitterSentimentLib as :libfile;
CREATE FUNCTION sentiment as language 'C++' name 'SenAnalyseSimpleFactory' library TwitterSentimentLib;
CREATE TRANSFORM FUNCTION twitter as language 'C++' name 'TwitterJSONFactory' library TwitterSentimentLib;
