
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
}

//virtual RC getNextTuple(void *data) = 0;
/* should set output parameter data for the next record.
 * the format of the data parameter, which refers to the next tuple
 * of the operator's output, is the same as that used in previous
 * projects*/


//virtual void getAttributes(vector<Attribute> &attrs) const = 0;
/* This method returns a vector of attributes in the intermediate
 * relation resulted from thsi iterator. That is, while the previous
 * method returns the tuples from the operator, this method makes
 * the associated schema information for the returned tuple stream
 * available in the query plan. The names of the attributes in the 
 * vector<Attribute> should be of the form relation.attribute to
 * clearly specify the relation from which each attribute comes.*/

// ... the rest of your implementations go here
RC Filter::getNextTuple(void *data) {return QE_EOF;};
// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const{};
