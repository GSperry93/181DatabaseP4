
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
		for(int i = 0; i < attr.size(); i++)
		{
			int size; 
			int offset = 0;
			
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

	}
	while(!condTrue && iter != NULL);
	
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


/*foreach tuple r in R do
 * 	foreach tuple s in S where r(i) == s(i) do
 * 		add <r,s> to result
 */
INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition)   // Join condition
        
{
	left = leftIn;
	right = rightIn;
	cond = condition;	
}
        
INLJoin::~INLJoin()
{

}

RC INLJoin::getNextTuple(void *data)
{
	
	return QE_EOF;
}
// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const
{

}
