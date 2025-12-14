/**
 * @file insert.C
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
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string &relation, const int attrCnt, const attrInfo attrList[]) {
    std::cout << "Doing QU_Insert " << endl;

    if (relation.empty()) {
        return BADCATPARM;
    }

    Status status;
    int relAttrCnt = 0;
    AttrDesc *relAttrs = NULL;

    status = attrCat->getRelInfo(relation, relAttrCnt, relAttrs);
    if (status != OK) {
        return status;
    }

    if (attrCnt != relAttrCnt) {
        free(relAttrs);
        return BADCATPARM;
    }

    int reclen = 0;
    for (int i = 0; i < relAttrCnt; i++) {
        reclen += relAttrs[i].attrLen;
    }

    std::unique_ptr<char[]> recordData(new (std::nothrow) char[reclen]);
    if (!recordData) {
        free(relAttrs);
        return INSUFMEM;
    }
    memset(recordData.get(), 0, reclen);

    std::unique_ptr<bool[]> used(new (std::nothrow) bool[attrCnt]);
    if (!used) {
        free(relAttrs);
        return INSUFMEM;
    }
    for (int i = 0; i < attrCnt; i++) {
        used[i] = false;
    }

    for (int i = 0; i < relAttrCnt; i++) {
        int found = -1;
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(relAttrs[i].attrName, attrList[j].attrName) == 0) {
                found = j;
                break;
            }
        }
        if (found == -1) {
            free(relAttrs);
            return ATTRNOTFOUND;
        }
        if (used[found]) {
            free(relAttrs);
            return BADCATPARM;
        }
        used[found] = true;

        if (attrList[found].attrType != relAttrs[i].attrType) {
            free(relAttrs);
            return ATTRTYPEMISMATCH;
        }

        char *value = (char *)attrList[found].attrValue;
        switch (relAttrs[i].attrType) {
            case INTEGER: {
                int temp = atoi(value);
                memcpy(recordData.get() + relAttrs[i].attrOffset, &temp, sizeof(int));
                break;
            }
            case FLOAT: {
                float temp = (float)atof(value);
                memcpy(recordData.get() + relAttrs[i].attrOffset, &temp, sizeof(float));
                break;
            }
            case STRING: {
                int len = strlen(value);
                if (len > relAttrs[i].attrLen) {
                    free(relAttrs);
                    return ATTRTOOLONG;
                }
                memcpy(recordData.get() + relAttrs[i].attrOffset, value, len);
                break;
            }
        }
    }

    InsertFileScan inserter(relation, status);
    if (status != OK) {
        free(relAttrs);
        return status;
    }

    Record rec;
    rec.data = recordData.get();
    rec.length = reclen;
    RID rid;
    status = inserter.insertRecord(rec, rid);

    free(relAttrs);
    return status;
}
