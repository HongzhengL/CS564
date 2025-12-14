/**
 * @file select.C
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
#include "stdio.h"
#include "stdlib.h"
#include "string.h"

// forward declaration
const Status ScanSelect(const string &result, const int projCnt, const AttrDesc projNames[], const AttrDesc *attrDesc,
                        const Operator op, const char *filter, const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string &result, const int projCnt, const attrInfo projNames[], const attrInfo *attr,
                       const Operator op, const char *attrValue) {
    std::cout << "Doing QU_Select " << endl;

    if (projCnt < 1) {
        return BADCATPARM;
    }

    Status status;

    std::unique_ptr<AttrDesc[]> projDescs(new (std::nothrow) AttrDesc[projCnt]);
    if (!projDescs) {
        return INSUFMEM;
    }

    int reclen = 0;
    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projDescs[i]);
        if (status != OK) {
            return status;
        }
        reclen += projDescs[i].attrLen;
    }

    AttrDesc attrDesc;
    AttrDesc *selDesc = NULL;
    const char *filter = NULL;
    int intVal = 0;
    float floatVal = 0.0;
    std::unique_ptr<char[]> stringVal;

    if (attr != NULL) {
        status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
        if (status != OK) {
            return status;
        }

        if (attrDesc.attrType != attr->attrType) {
            return ATTRTYPEMISMATCH;
        }

        selDesc = &attrDesc;
        switch (attrDesc.attrType) {
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

    status = ScanSelect(result, projCnt, projDescs.get(), selDesc, op, filter, reclen);

    return status;
}

const Status ScanSelect(const string &result, const int projCnt, const AttrDesc projNames[], const AttrDesc *attrDesc,
                        const Operator op, const char *filter, const int reclen) {
    std::cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status;
    Status finalStatus = OK;

    std::unique_ptr<char[]> outData(new (std::nothrow) char[reclen]);
    if (!outData) {
        return INSUFMEM;
    }
    Record outRec;
    outRec.data = outData.get();
    outRec.length = reclen;

    InsertFileScan resultRel(result, status);
    if (status != OK) {
        return status;
    }

    string inRelName = string(projNames[0].relName);
    HeapFileScan inRel(inRelName, status);
    if (status != OK) {
        return status;
    }

    if (attrDesc == NULL) {
        status = inRel.startScan(0, 0, STRING, NULL, EQ);
    } else {
        status = inRel.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
    }

    if (status != OK) {
        return status;
    }

    RID rid;
    Record inRec;
    while ((status = inRel.scanNext(rid)) == OK) {
        status = inRel.getRecord(inRec);
        if (status != OK) {
            finalStatus = status;
            break;
        }

        int offset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(outData.get() + offset, (char *)inRec.data + projNames[i].attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }

        RID outRid;
        status = resultRel.insertRecord(outRec, outRid);
        if (status != OK) {
            finalStatus = status;
            break;
        }
    }

    if (status != FILEEOF && finalStatus == OK) {
        finalStatus = status;
    }

    return finalStatus;
}
