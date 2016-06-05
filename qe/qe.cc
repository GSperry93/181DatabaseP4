
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) 
{
	iter = input;
	cond = condition;	

}

// ... the rest of your implementations go here
RC Filter::getNextTuple(void *data) 
{
	void* tempData = malloc(4096);
	void* value = malloc (4096);
	bool condTrue;
	vector<Attribute> attr;
	getAttributes(attr);
	
	for(int i = 0; i < attr.size(); i++)
	{
		if (attr[i].name == cond.lhsAttr) {lhsAttrNum = i;}
		if (cond.bRhsIsAttr)
		{
			if(attr[i].name == cond.rhsAttr) {rhsAttrNum = i;}
		}		
	}

	lhsAttrVal = malloc(attr[lhsAttrNum].length + 1);
	if(cond.bRhsIsAttr)
	{
		rhsAttrVal = malloc(attr[rhsAttrNum].length + 1);
	}
	else 
	{
		if(cond.rhsValue.type == TypeInt || cond.rhsValue.type == TypeReal)
		{
			rhsAttrVal = malloc(4);
			memcpy(rhsAttrVal, cond.rhsValue.data, 4);
		}
		else
		{
			rhsAttrVal = malloc(*(int*)cond.rhsValue.data + 1);
			memcpy(rhsAttrVal, (void*)((char*)cond.rhsValue.data+4), (*(int*)cond.rhsValue.data));
			*((char*)rhsAttrVal + *(int*)cond.rhsValue.data) = '\0';
		}
		 
	}

	do
	{
		if(iter->getNextTuple(tempData) == QE_EOF)
		{
			return QE_EOF;
		}	
		//read lhs value out of tuple
		//read rhs attr value if applicable
		int nullbytes = ceil((float)attr.size()/8.0);
		void* nulls = malloc(nullbytes);
		memcpy(nulls, tempData, nullbytes);
		
		int offset = nullbytes;
		
		for(int i = 0; i < attr.size(); i++)
		{
			int size = 0; 
    			int indicatorIndex = i / CHAR_BIT;
    			int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
   			if ((((char*)nulls)[indicatorIndex] & indicatorMask) != 0)
			{
				if(attr[i].type == TypeInt || attr[i].type == TypeReal)
				{
					size = 4;
					memcpy(value, (void*)((char*)tempData+offset), size);
					offset = offset + size;
				}
				else
				{
					memcpy(value, (void*)((char*)tempData+offset+4), *((int*)tempData+offset));
					offset = offset + size;
					*((char*)value + *((int*)tempData+offset)) = '\0';
					size++;
				}
			 
				if(i == lhsAttrNum)
				{
					memcpy(lhsAttrVal, value, size);
				}
				if(i == rhsAttrNum)
				{
					memcpy(rhsAttrVal, value, size);
				}
			}
		}	
		//else read rhsValue into rhsAttrValue
		if(attr[lhsAttrNum].type == TypeInt)
		{
			condTrue = checkCond(*(int*)lhsAttrVal);
		}
		else if(attr[lhsAttrNum].type == TypeReal)
		{
			condTrue = checkCond(*(float*)lhsAttrVal);
		}
		else if(attr[lhsAttrNum].type == TypeVarChar)
		{
			condTrue = checkCond((char*)lhsAttrVal);
		}		

	}while(!condTrue && iter != NULL);
	
	free(tempData);
	free(value);	

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
			if(intCond == *(int*)rhsAttrVal) {return true;}
			else {return false;}
			break;	
		case LT_OP : 
			if(intCond < *(int*)rhsAttrVal) {return true;}
			else {return false;}
			break;	
		case LE_OP : 
			if(intCond <= *(int*)rhsAttrVal) {return true;}
			else {return false;}
			break;	
		case GT_OP : 
			if(intCond > *(int*)rhsAttrVal) {return true;}
			else {return false;}
			break;	
		case GE_OP : 
			if(intCond >= *(int*)rhsAttrVal) {return true;}
			else {return false;}
			break;	
		case NE_OP :
			if(intCond != *(int*)rhsAttrVal) {return true;}
			else {return false;}
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
			if(realCond == *(float*)rhsAttrVal) {return true;}
			else {return false;}
			break;	
		case LT_OP : 
			if(realCond < *(float*)rhsAttrVal) {return true;}
			else {return false;}
			break;	
		case LE_OP : 
			if(realCond <= *(float*)rhsAttrVal) {return true;}
			else {return false;}
			break;	
		case GT_OP : 
			if(realCond > *(float*)rhsAttrVal) {return true;}
			else {return false;}
			break;	
		case GE_OP : 
			if(realCond >= *(float*)rhsAttrVal) {return true;}
			else {return false;}		
			break;	
		case NE_OP :
			if(realCond != *(float*)rhsAttrVal) {return true;}
			else {return false;}
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
		strcmpRes = strcmp(charCond, (char*)rhsAttrVal);

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
void INLJoin::parseTuple(void *innerData, vector<Attribute> innerAttributes, string compAttr, char *stringResult, int32_t &numResult){
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    char *cursor = (char *)innerData;
    char *nullIndicator = (char *)innerData;
    int nullSize = rbfm->getNullIndicatorSize(innerAttributes.size());
    cursor += nullSize;
    int i = 0;
    Attribute desired;
    for(auto a: innerAttributes){
        if(a.name.compare(compAttr) == 0){
            desired = a;
            break;
        }
        if(rbfm->fieldIsNull(nullIndicator, i)){
            i++;
            continue;
       }
       if(a.type == TypeVarChar){
            int32_t length;
            memcpy(&length, cursor, 4);
            cursor += 4 + length;
        }else{
            cursor += 4;
        }
        i++;
    }
    if(desired.type == TypeVarChar){
        memcpy(&numResult, cursor, 4);
        cursor += 4;
        // +1 for null termination
        stringResult = (char *)malloc(numResult+1);
        memcpy(stringResult, cursor, numResult);
        *(char *)(stringResult + numResult) = '\0';
    }else{
        memcpy(&numResult, cursor, 4);
    }
}
/*foreach tuple r in R do
 * 	foreach tuple s in S where r(i) == s(i) do
 * 		add <r,s> to result
 */
INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition)   // Join condition
        
{
        vector<Attribute> outerAttributes;
        vector<Attribute> innerAttributes;
        leftIn->getAttributes(outerAttributes);
        rightIn->getAttributes(innerAttributes);
        vector<Attribute> combinedAttributes;
        combinedAttributes = outerAttributes;
        failFlag = false;
        sameAttributeName = false;
        if(!condition.bRhsIsAttr){
            // You cannot join on a non attribute
            failFlag = true;
            return;
        }
        if(condition.lhsAttr.compare(condition.rhsAttr) == 0){
            sameAttributeName = true; 
        }
        Attribute outerAttribute;
        Attribute innerAttribute;
        for(auto a: innerAttributes){
            if(sameAttributeName && a.name.compare(condition.lhsAttr) != 0){
                combinedAttributes.push_back(a); 
            }else if(a.name.compare(condition.rhsAttr) == 0){
                innerAttribute = a;
                combinedAttributes.push_back(a);
            }
            else{
                combinedAttributes.push_back(a);
            }
        }
        for(auto a: outerAttributes){
            if(a.name.compare(condition.lhsAttr) == 0){
                outerAttribute = a;
                break;
            }
        }
        RelationManager *rm = RelationManager::instance();
        RC rc = rm->createTable("joinTable", combinedAttributes);
        if(rc)
          failFlag = true;
        void *data = malloc(PAGE_SIZE);
        while(leftIn->getNextTuple(data) != -1){
            char *strData = NULL;
            int32_t numData = 0;
            parseTuple(data, outerAttributes, condition.lhsAttr, strData, numData);
            void *innerData = malloc(PAGE_SIZE);
            while(rightIn->getNextTuple(innerData) != -1){
                char *innerStrData = NULL;
                int32_t innerNumData = 0;
                parseTuple(innerData, innerAttributes, condition.rhsAttr, innerStrData, innerNumData);
                bool addFlag = false;
                if(innerAttribute.type == TypeVarChar){
                    if(strcmp(strData, innerStrData) == 0){
                        addFlag = true;
                    }
                } else{
                    if(numData == innerNumData)
                      addFlag = true;
                }
                free(innerStrData);
            }
            free(strData);
            free(innerData);
        }
        free(data);
}
        
INLJoin::~INLJoin()
{

}

void * INLJoin::mergeTuples(void * tupleOne, void * tupleTwo, vector<Attribute> oneAttrs, vector<Attribute> twoAttrs, string name)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	void * data = malloc(PAGE_SIZE);
	int dataOffset = rbfm->getNullIndicatorSize(oneAttrs.size()+twoAttrs.size()-1);
	int nullSize = dataOffset;

	int oneOffset = rbfm->getNullIndicatorSize(oneAttrs.size());
	int twoOffset = rbfm->getNullIndicatorSize(twoAttrs.size());

	void * oneNullBytes = malloc(oneOffset);
	memset(oneNullBytes, 0, oneOffset);
	memcpy(oneNullBytes, tupleOne, oneOffset);

	void * twoNullBytes = malloc(twoOffset);
	memset(twoNullBytes, 0, twoOffset);
	memcpy(twoNullBytes, tupleTwo, twoOffset);

	void * dataNullBytes = malloc(dataOffset);
	memset(dataNullBytes, 0, dataOffset);

	for(int i = 0; i<oneAttrs.size(); i++){
		if(rbfm->fieldIsNull(oneNullBytes, i )){
	        int indicatorIndex = (i+1) / CHAR_BIT;
        	int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        	dataNullBytes[indicatorIndex] |= indicatorMask;
		}else{
			if(oneAttrs[i].type == TypeVarChar){
				void * len = malloc(4);
				memcpy(len, (char*)oneNullBytes+oneOffset, 4);
				memcpy((char*)data+dataOffset, (char*)oneNullBytes+oneOffset, (*len)+4);
				dataOffset+=(*len)+4;
				oneOffset+=(*len)+4;
			}else{
				memcpy((char*)data+dataOffset, (char*)oneNullBytes+oneOffset, 4);
				dataOffset += 4;
				oneOffset += 4;
			}
		}
	}
	int j = oneAttrs.size();
	for(int i = 0; i<twoAttrs.size(); i++){
		if(twoAttrs[i].name.compare(name) ==  0){
			//this attribute already in result :)
			continue;
		}
		if(rbfm->fieldIsNull(twoNullBytes, i )){
	        int indicatorIndex = (j+1) / CHAR_BIT;
	        int indicatorMask  = 1 << (CHAR_BIT - 1 - (j % CHAR_BIT));
	        dataNullBytes[indicatorIndex] |= indicatorMask;
		}else{
			j++;
			if(twoAttrs[i].type == TypeVarChar){
				void * len = malloc(4);
				memcpy(len, (char*)twoNullBytes+twoOffset, 4);
				memcpy((char*)data+dataOffset, (char*)twoNullBytes+twoOffset, (*len)+4);
				dataOffset+=(*len)+4;
				twoOffset+=(*len)+4;
			}else{
				memcpy((char*)data+dataOffset, (char*)twoNullBytes+twoOffset, 4);
				dataOffset += 4;
				twoOffset += 4;
			}
		}
	}
	memcpy(data, dataNullBytes, nullSize);
	return data;
}

RC INLJoin::mergeAttributes(vector<Attribute> one, vector<Attribute> two, string name, int &index)
{
	vector<Attribute> three;
	// one of the attrs is in both & we don't want to duplicate it
	three.reserve(one.size()+two.size()-1)
	for(int i = 0 ; i < one.size(); i++{
		three[i] = one[i]
	}
	int j = one.size();
	for(int i = 0 ; i < two.size(); i++{
		if(two[i].name.compare(name) ==  0){
			three[j] = two[i]
			j++
			index = i;
		}
	}
	return three;
}

RC INLJoin::getNextTuple(void *data)
{
	
	return QE_EOF;
}
// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const
{

}
