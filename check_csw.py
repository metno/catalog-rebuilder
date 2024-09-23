import re
import requests
from owslib.csw import CatalogueServiceWeb


def checkParents(parentList, csw_url):
    csw = CatalogueServiceWeb(csw_url)
    csw.identification.type
    [op.name for op in csw.operations]
    csw.getdomain('GetRecords.resultType')
    results = dict()
    i = 1
    for parent in parentList:
        csw.getrecordbyid(id=[parent])
        try:
            id = csw.records[parent].identifier
            title = csw.records[parent].title
            type = csw.records[parent].type
            if parent != id:
                return False
            if type == 'series':
                isParent = True
            else:
                isParent = False
                return False
            pdict = {"id": id, "title": title, "type": type, "isParent": isParent, "status": True}
            results[parent] = pdict
        except KeyError:
            pdict = {"id": parent, "status": False}
            results[parent] = pdict
            return False
        finally:
            i += 1

    res_keys = list(results.keys())
    for key in res_keys:
        elem = results[key]
        if elem['status'] is False:
            # print('Missing MMD parent record with id', elem['id'])
            return False
        if 'isParent' in elem:
            if elem['isParent'] is False:
                # print('MMD with id %s have not been marked as parent (i.e type should be series,
                #  is dataset)'% elem['id'])
                return False

    for parent in parentList:
        url = (f"{csw_url}?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetRecords&"
               "RESULTTYPE=results&TYPENAMES=csw:Record&ElementSetName=full&"
               "outputFormat=application%2Fxml&outputschema=http://www.isotc211.org"
               "/2005/gmd&CONSTRAINTLANGUAGE=CQL_TEXT&CONSTRAINT="
               f"apiso:ParentIdentifier%20like%20%27{parent}%27")
        r = requests.get(url)
        if r.status_code != 200 or "ExceptionReport" in r.text:
            # print(f"{parent}: CSW search for children is failing!")
            return False
        else:
            m = re.search(r".*numberOfRecordsMatched=\"(\d+)\".*", r.text)
            if not m:
                return False

    for ii in range(len(parentList)):
        url = (f"{csw_url}?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetRecords&RESULTTYPE=results"
               "&TYPENAMES=csw:Record&ElementSetName=full&outputschema="
               "http://www.isotc211.org/2005/gmd&CONSTRAINTLANGUAGE=CQL_TEXT&CONSTRAINT="
               f"dc:type%20like%20%27series%27&startposition={ii+1}")
        r = requests.get(url)
        if r.status_code != 200 or "ExceptionReport" in r.text:
            # print(f"{parentList[ii]}: CSW search starting at index {ii+1} fails!")
            return False
    return True
