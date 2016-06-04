
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) 
{
	iter = input;
	cond = condition;	

}

//virtual RC getNextTuple(void *data) = 0;
/* should set output parameter data for the next record.
 * the format of the data parameter, which refers to the next tuple
 * of the operator's output, is the same as that used in previous
 * projects
 */


//virtual void getAttributes(vector<Attribute> &attrs) const = 0;
/* This method returns a vector of attributes in the intermediate
 * relation resulted from this iterator. That is, while the previous
 * method returns the tuples from the operator, this method makes
 * the associated schema information for the returned tuple stream
 * available in the query plan. The names of the attributes in the 
 * vector<Attribute> should be of the form relation.attribute to
 * clearly specify the relation from which each attribute comes.i
 */

// ... the rest of your implementations go here
RC Filter::getNextTuple(void *data) 
{
	
	
	
//struct Condition {
//    string  lhsAttr;        // left-hand side attribute
//    CompOp  op;             // comparison operator
//    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
//    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
//    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE

// 
//    EQ_OP = 0,  // no condition// = 
//    LT_OP,      // <
//    LE_OP,      // <=
//    GT_OP,      // >
//    GE_OP,      // >=
//    NE_OP,      // !=
//    NO_OP       // no condition
//} CompOp;
	void* tempData = malloc(4096);
	bool condTrue;
	

	do
	{
		if(iter->getNextTuple(tempData) == QE_EOF)
		{
			return QE_EOF;
		}	
		//read lhs value out of tuple

		vector<Attribute> attr;
		getAttributes(attr);
		for(int i = 0; i < attr.size(); i++
		{
			
		}
		//read rhs attr value if applicable

	}
	while(!condTrue && iter != NULL);
	if(condTrue)
	{
		data = tempData;
		return 0;
	}
	
	return QE_EOF;
}


bool Filter::checkCond(const int intCond)
{
	switch(cond.op)
	{
		case EQ_OP : 
			if(cond.bRhsIsAttr)
			{
				if(intCond == *(int*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(intCond == cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case LT_OP : 
			if(cond.bRhsIsAttr)
			{
				if(intCond < *(int*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(intCond < cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case LE_OP : 
			if(cond.bRhsIsAttr)
			{
				if(intCond <= *(int*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(intCond <= cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case GT_OP : 
			if(cond.bRhsIsAttr)
			{
				if(intCond > *(int*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(intCond > cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case GE_OP : 
			if(cond.bRhsIsAttr)
			{
				if(intCond >= *(int*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(intCond >= cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case NE_OP :
			if(cond.bRhsIsAttr)
			{
				if(intCond != *(int*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(intCond != cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case NO_OP :
			return true;
	}
	return false;
}
bool Filter::checkCond(const float realCond)
{
	switch(cond.op)
	{
		case EQ_OP : 
			if(cond.bRhsIsAttr)
			{
				if(realCond == *(float*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(realCond == cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case LT_OP : 
			if(cond.bRhsIsAttr)
			{
				if(realCond < *(float*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(realCond < cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case LE_OP : 
			if(cond.bRhsIsAttr)
			{
				if(realCond <= *(float*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(realCond <= cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case GT_OP : 
			if(cond.bRhsIsAttr)
			{
				if(realCond > *(float*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(realCond > cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case GE_OP : 
			if(cond.bRhsIsAttr)
			{
				if(realCond >= *(float*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(realCond >= cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case NE_OP :
			if(cond.bRhsIsAttr)
			{
				if(realCond != *(float*)rhsAttrVal) {return true;}
				else {return false;}
				
			}
			else
			{
				if(realCond != cond.rhsValue) {return true;}
				else {return false;}
			}
			break;	
		case NO_OP :
			return true;
	}
	return false;

}

//will definitely segfault if the rhsAttr is not null terminated, which includes if it's improperly assumed to be a string when
//it is actually an int or a real
bool Filter::checkCond(const char* charCond)
{
	int strcmpRes;
	if(cond.bRhsIsAttr)
	{
		strcmpRes = strcmp(charCond, (char*)rhsAttrVal);
	}
	else
	{
		strcmpRes = strcmp(charCond, comp.rhsValue);
	}

	switch(cond.op)
	{
		case EQ_OP :
			if(strcmpRes == 0) {return true;}
			else {return false;} 
			break;	
		case LT_OP : 
			if(strcmpRes < 0) {return true;}
			else {return false;}
			break;	
		case LE_OP : 
			if(strcmpRes <= 0) {return true;}
			else {return false;}
			break;	
		case GT_OP : 
			if(strcmpRes > 0) {return true;}
			else {return false;}
			break;	
		case GE_OP : 
			if(strcmpRes >= 0) {return true;}
			else {return false;}
			break;	
		case NE_OP :
			if(strcmpRes != 0) {return true;}
			else {return false;}
			break;	
		case NO_OP :
			return true;
	}
	return false;

}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const
{
	iter->getAttributes(attrs);
}


/*class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

	//Scans a table with tableName and puts it into an iterator named iter
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
*/
