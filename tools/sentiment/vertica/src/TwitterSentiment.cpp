#include "Vertica.h"
#include "Words.h"
#include "json/json.h"
#include <vector>
#include <sstream>

using namespace Vertica;

/*
 * This is a simple function that calculte the sentiment by
 * summing up the scores of all sentiment words in a string.
 *
 * @author andy
 */
    std::string Mon = "Mon";
    std::string Tue = "Tue";
    std::string Wed = "Wed";
    std::string Thu = "Thu";
    std::string Fri = "Fri";
    std::string Sat = "Sat";
    std::string Sun = "Sun";

    std::string one = "1";
    std::string two = "2";
    std::string three = "3";
    std::string four = "4";
    std::string five = "5";
    std::string six = "6";
    std::string seven = "7";

static inline const std::string getStringSafe(const VString &vstr) 
{
    return (vstr.isNull() || vstr.length() == 0)  ? "" : vstr.str();
}

int inline findAndReplace(std::string& source, const std::string& find, const std::string& replace)
{
    std::size_t fLen = find.size();
    std::size_t rLen = replace.size();
    std::size_t pos = source.find(find);
    if(pos != std::string::npos)
    {
        source.replace(pos, fLen, replace);
        return 1;
    }
    return 0;
}


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


/*
 * Parse the Twitter JSON results
 */
class TwitterJSONParser : public TransformFunction
{
    Json::Value root;   // will contains the root value after parsing.
    Json::Reader reader;

public:

    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &input,
                                  PartitionWriter &output)
    {
        // Basic error checking
        if (input.getNumCols() != 1)
            vt_report_error(0, "Function only accept 1 arguments, but %zu provided", 
                            input.getNumCols());

        // While we have inputs to process
        do {
            // Get a copy of the input string
            std::string  inStr = getStringSafe(input.getStringRef(0));

            bool success = reader.parse(inStr, root, false);
            if (!success)
                vt_report_error(0, "Malformed JSON text");
                
     
            std::string created_at = root.get("created_at", "").asString();
            std::string text = root.get("text", "").asString();
            if (findAndReplace(created_at, Sun, one) == 0)
                if(findAndReplace(created_at, Mon, two) == 0)
                    if(findAndReplace(created_at, Tue, three) == 0)
                        if(findAndReplace(created_at, Wed, four) == 0)
                            if(findAndReplace(created_at, Thu, five) == 0)
                                if(findAndReplace(created_at, Fri, six) == 0)
                                    findAndReplace(created_at, Sat, seven);

            Json::Value hastags = root["entities"]["hashtags"];
            int user_id = root["user"]["id"].asInt();

            for (uint i=0; i<hastags.size(); i++) {
                std::string tag = hastags[i].asString();

                // Copy string into results
                output.setInt(0, user_id);
                output.getStringRef(1).copy(tag);
                output.getStringRef(2).copy(created_at);
                output.getStringRef(3).copy(text);
                output.next();
            }
        } while (input.next());
    }
};

class TwitterJSONFactory : public TransformFunctionFactory
{
    // return an instance of the transform function
    virtual TransformFunction *createTransformFunction(ServerInterface &interface)
    { return vt_createFuncObj(interface.allocator, TwitterJSONParser); }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addVarchar();
        returnType.addInt();
        returnType.addVarchar();
        returnType.addVarchar();
        returnType.addVarchar();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType)
    {
        returnType.addInt("user_id");
        returnType.addVarchar(200, "hashtag"); 
        returnType.addVarchar(200, "created_at"); 
        returnType.addVarchar(1000, "text");
    }
};

RegisterFactory(TwitterJSONFactory);
