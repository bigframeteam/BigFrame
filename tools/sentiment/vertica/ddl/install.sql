-- Installation script: defined the shared library and the appropriate entry points

select version();

\set libfile '\''`pwd`'/lib/SenAnalyseSimple.so\'';

CREATE LIBRARY SenAnalyseSimpleLib as :libfile;
CREATE FUNCTION sentiment as language 'C++' name 'SenAnalyseSimpleFactory' library SenAnalyseSimpleLib;



