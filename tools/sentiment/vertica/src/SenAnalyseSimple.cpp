#include "Vertica.h"
#include "Words.h"
#include <vector>
#include <sstream>

using namespace Vertica;

/*
 * This is a simple function that calculte the sentiment by
 * summing up the scores of all sentiment words in a string.
 *
 * @author andy
 */
class SenAnalyseSimple : public ScalarFunction
{

    static inline void tokenize(const std::string &text, std::vector<std::string> &tokens)
    {
		std::istringstream ss(text);

        do {
            std::string buffer;
            ss >> buffer;

            if (!buffer.empty()) 
                tokens.push_back(buffer);
        } while (ss);
        
    }

public:

    /*
     * This method processes a block of rows in a single invocation.
     *
     * The inputs are retrieved via argReader
     * The outputs are returned via resWriter
     */
    virtual void processBlock(ServerInterface &srvInterface,
                              BlockReader &argReader,
                              BlockWriter &resWriter)
    {
        try {
            // Basic error checking
            if (argReader.getNumCols() != 1)
                vt_report_error(0, "Function only accept 1 arguments, but %zu provided", 
                                argReader.getNumCols());

            // While we have inputs to process
            do {
                // Get a copy of the input string
                std::string  inStr = argReader.getStringRef(0).str();

				std::vector< std::string > words;
				tokenize(inStr, words);
				
				int score = 0;

				for(int i = 0; i < words.size(); i++) {
					if(sen_dict.find(words[i]) != sen_dict.end()) {
						score += sen_dict.find(words[i])->second;
					}
				}
				
				// Copy scores into result
                resWriter.setInt(score);
                resWriter.next();
            } while (argReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class SenAnalyseSimpleFactory : public ScalarFunctionFactory
{
    // return an instance of SenAnalyseSimple to perform the actual sentiment
	// analysis.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
    { return vt_createFuncObj(interface.allocator, SenAnalyseSimple); }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addVarchar();
        returnType.addInt();
    }
};


RegisterFactory(SenAnalyseSimpleFactory);

