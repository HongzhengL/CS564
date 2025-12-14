/**
 * @file delete.C
 * @author Bobby Tang (tang287)
 * @author Hongzheng Li (hli2225)
 * @author Junnan Li (jli2786)
 * @version 0.1
 * @date 2025-12-14
 *
 * @copyright Copyright (c) 2025
 *
 */
#include <memory>
#include <new>

#include "catalog.h"
#include "query.h"
#include "string.h"
#include "stdlib.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string &relation, const string &attrName, const Operator op, const Datatype type,
                       const char *attrValue) {
    std::cout << "Doing QU_Delete " << endl;

    if (relation.empty()) {
        return BADCATPARM;
    }

    Status status;
    AttrDesc attrDesc;
    AttrDesc *selDesc = NULL;
    const char *filter = NULL;
    int intVal = 0;
    float floatVal = 0.0;
    std::unique_ptr<char[]> stringVal;

    if (!attrName.empty()) {
        status = attrCat->getInfo(relation, attrName, attrDesc);
        if (status != OK) {
            return status;
        }

        if (attrDesc.attrType != type) {
            return ATTRTYPEMISMATCH;
        }

        selDesc = &attrDesc;
        switch (type) {
            case INTEGER:
                intVal = atoi(attrValue);
                filter = (char *)&intVal;
                break;
            case FLOAT:
                floatVal = (float)atof(attrValue);
                filter = (char *)&floatVal;
                break;
            case STRING:
                if ((int)strlen(attrValue) > attrDesc.attrLen) {
                    return ATTRTOOLONG;
                }
                stringVal.reset(new (std::nothrow) char[attrDesc.attrLen]);
                if (!stringVal) {
                    return INSUFMEM;
                }
                memset(stringVal.get(), 0, attrDesc.attrLen);
                strncpy(stringVal.get(), attrValue, attrDesc.attrLen);
                filter = stringVal.get();
                break;
        }
    }

    HeapFileScan scan(relation, status);
    if (status != OK) {
        return status;
    }

    if (selDesc == NULL) {
        status = scan.startScan(0, 0, STRING, NULL, EQ);
    } else {
        status = scan.startScan(selDesc->attrOffset, selDesc->attrLen, (Datatype)selDesc->attrType, filter, op);
    }

    if (status != OK) {
        return status;
    }

    RID rid;
    Status loopStatus;
    while ((loopStatus = scan.scanNext(rid)) == OK) {
        status = scan.deleteRecord();
        if (status != OK) {
            return status;
        }
    }

    if (loopStatus == FILEEOF) {
        return OK;
    }

    return loopStatus;
}
