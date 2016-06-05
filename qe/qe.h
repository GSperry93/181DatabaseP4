#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string.h>
#include <algorithm>
#include <math.h>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

     private:
	Condition cond;
	Iterator* iter;
	void* rhsAttrVal;
	void* lhsAttrVal;
	int lhsAttrNum, rhsAttrNum;

	bool checkCond(const int intCond);
	bool checkCond(const float realCond);
	bool checkCond(const char* varCharCond);
};


class Project : public Iterator {
    // Projection operator
    public:
        Iterator *input;
        vector<string> attrNames;
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames)
        {
            this->input = input;
            this->attrNames = attrNames;


        };   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data) 
        {
            if (attrNames.size() == 0)
            {
                return SUCCESS;
            }
            RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
            void * int_data = malloc(PAGE_SIZE);
            vector<Attribute> recordDescriptor;
            input->getAttributes(recordDescriptor);
            input->getNextTuple(int_data);
            //taken from rbfm

            // Prepare null indicator
            unsigned nullIndicatorSize = rbfm->getNullIndicatorSize(attrNames.size());
            char nullIndicator[nullIndicatorSize];
            memset(nullIndicator, 0, nullIndicatorSize);

            // Keep track of offset into data
            unsigned dataOffset = nullIndicatorSize;

            for (unsigned i = 0; i < attrNames.size(); i++)
            {
                // Get index and type of attribute in record
                auto pred = [&](Attribute a) {return a.name == attrNames[i];};
                auto iterPos = find_if(recordDescriptor.begin(), recordDescriptor.end(), pred);
                unsigned index = distance(recordDescriptor.begin(), iterPos);
                if (index == recordDescriptor.size())
                    return RBFM_NO_SUCH_ATTR;
                AttrType type = recordDescriptor[index].type;

                // Read attribute from int_data into buffer
                int bufOffset = nullIndicatorSize;
                for(int k = 0 ; k < index; k++){
                    AttrType temp_type = recordDescriptor[k].type;
                    bufOffset += getOffset(temp_type, int_data, bufOffset);
                }
                int size = getSize(type, int_data, bufOffset);
                void * buffer = malloc(PAGE_SIZE);
                memcpy(buffer, int_data, size);

                // Determine if null
                char null;
                memcpy (&null, buffer, 1);
                if (null)
                {
                    int indicatorIndex = i / CHAR_BIT;
                    char indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
                    nullIndicator[indicatorIndex] |= indicatorMask;
                }
                // Read from buffer into data
                else if (type == TypeInt)
                {
                    memcpy ((char*)data + dataOffset, (char*)buffer + 1, INT_SIZE);
                    dataOffset += INT_SIZE;
                }
                else if (type == TypeReal)
                {
                    memcpy ((char*)data + dataOffset, (char*)buffer + 1, REAL_SIZE);
                    dataOffset += REAL_SIZE;
                }
                else if (type == TypeVarChar)
                {
                    uint32_t varcharSize;
                    memcpy(&varcharSize, (char*)buffer + 1, VARCHAR_LENGTH_SIZE);
                    memcpy((char*)data + dataOffset, &varcharSize, VARCHAR_LENGTH_SIZE);
                    dataOffset += VARCHAR_LENGTH_SIZE;
                    memcpy((char*)data + dataOffset, (char*)buffer + 1 + VARCHAR_LENGTH_SIZE, varcharSize);
                    dataOffset += varcharSize;
                }
            }
            // Finally set null indicator of data, clean up and return
            memcpy((char*)data, nullIndicator, nullIndicatorSize);
            return SUCCESS;
        };
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const
        {
            vector<Attribute> recordDescriptor;
            input->getAttributes(recordDescriptor);
            vector<Attribute> to_return;
            to_return.reserve(attrNames.size());
            int j = 0;
            for(int i = 0 ; i < recordDescriptor.size(); i++){
                if(!recordDescriptor[i].name.compare(attrNames[j])){
                    //match, we want to project this attr
                    to_return[j] = recordDescriptor[i];
                    j++;
                }
            }

            attrs.clear();
            //replace with our projected attributes
            attrs = to_return;
        };
        int getOffset(AttrType type, void * data, int offset){
            int dataOffset =0;
            if (type == TypeInt)
                {
                    dataOffset += INT_SIZE;
                }
            else if (type == TypeReal)
                {
                    dataOffset += REAL_SIZE;
                }
            else if (type == TypeVarChar)
                {
                    uint32_t varcharSize;
                    memcpy(&varcharSize, (char*)data+offset, VARCHAR_LENGTH_SIZE);
                    dataOffset += varcharSize;
                    dataOffset += VARCHAR_LENGTH_SIZE;
                }
            return dataOffset;
        }
        int getSize(AttrType type, void * data, int offset){
            if (type == TypeInt)
                {
                    return INT_SIZE;
                }
            else if (type == TypeReal)
                {
                    return REAL_SIZE;
                }
            else if (type == TypeVarChar)
                {
                    uint32_t varcharSize;
                    int offSet = 0;
                    memcpy(&varcharSize, (char*)data+offset, VARCHAR_LENGTH_SIZE);
                    offSet += varcharSize;
                    offSet += VARCHAR_LENGTH_SIZE;
                    return offSet;
                }
            return 0;
        }
};

class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin();

        RC getNextTuple(void *data);

        void * mergeTuples(void * tupleOne, void * tupleTwo, vector<Attribute> oneAttrs, vector<Attribute> twoAttrs, string name, bool same);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
        vector<Attribute> mergeAttributes(vector<Attribute> one, vector<Attribute> two, string name, int &index);
        vector<Attribute> combinedAttribute;
    private:
        void parseTuple(void *innerData, vector<Attribute> innerAttributes, string compAttr, char *stringResult, int32_t &numResult);
	Iterator* left;
	IndexScan* right;
	Condition cond;
        bool failFlag;
        bool sameAttributeName;
};


#endif
