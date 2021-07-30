import fitz
from utils import *
import json
import numpy as np
import time 
import datetime
from dateutil import parser as pr
import datefinder
from fuzzywuzzy import fuzz

# from txtai.pipeline import Similarity

from elasticsearch import Elasticsearch, helpers

es = Elasticsearch(hosts=["http://localhost:9200"], timeout=60, retry_on_timeout=True)

class CFS(object):

    def __init__(self):
        self.pdf_path = None
        self.is_local = True
        self.info = {}
        self.reader = {
            'Tool' : self.tool_reader,
            'AI' : self.deeplearning_reader
        }
        self.pdf_info = {}

    def tool_reader(self, pdf_path):
        pdf_info = {}
        doc = fitz.open(pdf_path)
        p = 0
        for page in doc:
            pdf_info[f'page_{p + 1}'] = []
            blocks = json.loads(page.getText('json'))['blocks']
            blocks = sort_block(blocks)
            for block in blocks:
                text = get_text(block)
                pdf_info[f'page_{p + 1}'].append(text)
            p += 1
        return pdf_info

    def deeplearning_reader(self, pdf_path, check_annot=False):
        import easyocr
        reader = easyocr.Reader(['vi','en'], verbose=False, gpu = True)
        # doc = fitz.open(pdf_path)
        # p = 0
        pdf_info = {}
        # for page in doc:
        #     img = convert_img(page)
        #     img = np.array(img)
        #     result = reader.readtext(img, paragraph = True)
        #     pdf_info[f"page_{p + 1}"] = result
        #     p += 1
        p = 0
        t1 = time.time()
        images, annots, lsq_detect = pdfimage_process(pdf_path, check_annot=check_annot)
        print(time.time()-t1)
        # print(annots)
        # exit()
        results = []
        horizontal_results = []
        if check_annot:
            for p, annot in enumerate(annots):
                if annot is not None:
                    print('=='*30)
                    # print(annot)
                    result, horizontal_result = reader.readtext(annot, paragraph=False, cluster=True, page=p+1)
                    if result:
                        results.append(result)
                    if horizontal_result:
                        horizontal_results.append(horizontal_result)
                    # pdf_info[f"page_{p + 1}"] = result
                p += 1
        else:
            for p, img in enumerate(images):
                result, horizontal_result = reader.readtext(img, paragraph=False, cluster=True, page=p+1)
                if result:
                    results.append(result)
                if horizontal_result:
                    horizontal_results.append(horizontal_result)
                p+=1

        # print(horizontal_results)
        # for img in images:
        #     img = np.array(img)
        #     result = reader.readtext(img, paragraph = True)
        #     pdf_info[f"page_{p + 1}"] = result
        #     p += 1
        return results, horizontal_results, lsq_detect

    def get_ptn_info(self, pdf_info):
        result = {
            "registrant" : {
                "name" : "",
                "address" : ""
            },
            "factory" : {
                "name" : "",
                "address" : "",
            },
            "equipment" : ""
        }

        for page in pdf_info.keys():
            flag = False
            for text in pdf_info[page]:
                print(text)
                print("----------------------")
                if "1. Tên cơ sơ công bố" in text:
                    result['registrant']['name'] = text.split(':')[-1].strip()
                elif "2. Địa chỉ" in text:
                    result['registrant']['address'] = text.split('(')[0].split(':')[-1]
                if '(Sản xuất tại:' in text:
                    result['factory']['name'] = text.split('(')[-1].strip().split(";")[0].split(":")[-1].strip()
                    result['factory']['address'] = text.split(";")[-1].replace(')', '')
                elif "5. Tên trang thiết bị" in text:
                    flag = True
                    continue
                if flag:
                    result['equipment'] = text
                    flag = False
        return result

     
    def get_info(self, info):
        if isinstance(info, str):
            info = json.load(open(info, 'w'))
        
        key_info = {
            "equipment" : info['equipment'],
            "equipmentOwner" : info['equipmentOwner']
        }
        if len(info['cfsForeign']) > 0 and len(info['cfsLocal']) == 0:
            self.is_local = False

        
        if self.is_local:
            urls = info['cfsLocal']['files']
            for url in urls:
                pdf_path = save_pdf(url['url'])
                try:
                    pdf_info = self.reader['Tool'](pdf_path)
                except:
                    pdf_info = self.reader['AI'](pdf_path)
                self.pdf_info[f"file_{url[id]}"] = pdf_info


        else:
            urls = info['cfsForeign']['files']
            for url in urls:
                pdf_path = save_pdf(url['url'])
                pdf_info = self.reader['AI'](pdf_path)
                self.pdf_path[f'file_{url[id]}'] = pdf_info

    def final_result(self, query, result=None, ptn=False):
        comments = []
        err = []
        if ptn:
            if fuzz.token_set_ratio(result['Production_company'], query['registrant']['name']) > score:
                comments.append(
                    {
                        "commentContent" : "Tên, địa chỉ cơ sở sản xuât hợp lệ (trang 1)",
                        "commentStatus" : "OK"
                    }
                )
            
            else:
                comments.append(
                    {
                        "commentContent" : "Tên, địa chỉ cơ sở sản xuât không hợp lệ (trang 1)",
                        "commentStatus" : "NOK"
                    }

                )
                err.append(1)
            doc_type = 1
        
        else:
            ####
            # print(query["registrant"]['name'])
            name_equipmentFactory = search(query["registrant"]['name'])
            if name_equipmentFactory:
                fuzzy_score = fuzz.token_set_ratio(name_equipmentFactory[0], query["registrant"]['name'])
                if fuzzy_score>70:
                    comments.append({
                            "commentContent" : f"Công ty sản xuất hợp lệ (trang {name_equipmentFactory[0][-9:]})",
                            "commentStatus" : "OK",
                            "query": query['registrant']['name'],
                            "detail": name_equipmentFactory
                        })
                else:
                    comments.append({ 
                        "commentContent" : "Công ty sản xuất không hợp lệ",
                        "commentStatus" : "NOK",
                        "query": query['registrant']['name'],
                        "detail": name_equipmentFactory
                    })
                    err.append(2)
            else:
                comments.append({ 
                        "commentContent" : "Công ty sản xuất không hợp lệ",
                        "commentStatus" : "NOK",
                        "query": query['registrant']['name'],
                        "detail": f"Can not find {query['registrant']['name']}"
                    })
                err.append(2)
            ####
            name_equipmentOwner = search(query["equipmentOwner"]['name'])
            if name_equipmentOwner:
                fuzzy_score = fuzz.token_set_ratio(name_equipmentOwner[0], query["equipmentOwner"]['name'])
                if fuzzy_score>70:
                    comments.append({ 
                            "commentContent" : f"Công ty sở hữu hợp lệ (trang {name_equipmentOwner[0][-9:]})",
                            "commentStatus" : "OK",
                            "query": query['equipmentOwner']['name'],
                            "detail": name_equipmentOwner
                        })
                else:
                    comments.append({
                        "commentContent" : "Công ty sở hữu không hợp lệ",
                        "commentStatus" : "NOK",
                        "query": query['equipmentOwner']['name'],
                        "detail": name_equipmentOwner
                    })
                    err.append(3)
            else:
                comments.append({
                        "commentContent" : "Công ty sở hữu không hợp lệ",
                        "commentStatus" : "NOK",
                        "query": query['equipmentOwner']['name'],
                        "detail": f"cannot find {query['equipmentOwner']['name']}"
                    })
                err.append(3)

            ####
            # equipment_flag = 1
            eqt_flag = 1
            type_flag = 1
            equipments = []
            codes = []
            eqt_name = []
            eqt_code = []
            for idx, obj in enumerate(query['equipment']['equipmentList']):
                if eqt_flag:
                    try:
                        n_equipment = search(obj['name'])
                        eqt_name.append(obj['name'])
                        equipments.append(n_equipment)
                        if n_equipment:
                            # name_equipment.append(n_equipment)
                            if fuzz.token_set_ratio(n_equipment[0], obj['name'])<0.7:
                                eqt_flag = 0
                        else:
                            # comments.append({
                            #     "commentContent" : f"Danh sách TTBYT ko đúng",
                            #     "commentStatus" : "NOK"
                            # })
                            # comments.append({
                            #     "commentContent" : f"Danh sách mã TTBYT ko đúng",
                            #     "commentStatus" : "NOK"
                            # })
                            eqt_flag = 0
                    except:
                        eqt_flag = 0
                if type_flag:
                    try:   
                        for type_equipment in obj['code']:
                            t_equipment = search(type_equipment)
                            eqt_code.append(obj['code'])
                            codes.append(t_equipment)
                            if t_equipment:
                                if fuzz.token_set_ratio(t_equipment[0], type_equipment)<0.9:
                                    type_flag = 0
                            else:
                                type_flag = 0
                    except:
                        type_flag = 0

            if eqt_flag:
                comments.append({
                    "commentContent" : f"Danh sách TTBYT đầy đủ",
                    "commentStatus" : "OK",
                    "query": eqt_name,
                    "detail": equipments
                })    
            else:
                comments.append({
                    "commentContent" : f"Danh sách TTBYT ko đúng",
                    "commentStatus" : "NOK",
                    "query": eqt_name,
                    "detail": equipments
                })
                err.append(4)
            
            if type_flag:
                comments.append({
                            "commentContent" : f"Danh sách mã TTBYT đầy đủ",
                            "commentStatus" : "OK",
                            "query": eqt_code,
                            "detail": codes
                        })
            else:
                comments.append({
                            "commentContent" : f"Danh sách mã TTBYT ko đúng",
                            "commentStatus" : "NOK",
                            "query": eqt_code,
                            "detail": codes
                        })
                err.append(5)

            ####
            valid_date = []
            begin_date = []
            period_time = []
            valid_date.extend(search('expiry date', 3))
            valid_date.extend(search('valid until', 3))
            valid_date.extend(search('valid from to', 3))
            begin_date.extend(search('ngày', 3))
            begin_date.extend(search('date', 3))
            period_time.extend(search('valid for three 3 years from'))
            period_time.extend(search('có giá trị ba 3 năm từ'))
            
            # print(valid_date)
            date_flag = 0
            period_flag = 0
            date_end = []
            date_start = []
            if begin_date:
                for date in begin_date:
                    time_start = "".join(u for u in date if u not in ("?", ".", ";", ":", "!"))
                    try:
                        time_start = list(datefinder.find_dates(time_start.strip()))
                        date_start.extend(time_start)
                    except:
                        continue
            if valid_date:
                for date in valid_date:
                    time_end = "".join(u for u in date if u not in ("?", ".", ";", ":", "!"))
                    try:
                        time_end = list(datefinder.find_dates(time_end.strip()))
                        date_end.extend(time_end.strftime('%m/%d/%Y'))
                    except:
                        continue
                    for tim in time_end:
                        if tim > datetime.datetime.now():
                            comments.append({ 
                                "commentContent": f"Thời gian còn hiệu lực (trang {date[-9:]})",
                                "commentStatus" : "OK",
                                "date_end": date_end,
                                "date_start": [t.strftime('%m/%d/%Y') for t in date_start],
                                "valid_date": valid_date,
                                "begin_date": begin_date,
                                "period": period_time
                            })
                            date_flag = 1
                            break
            elif period_time:
                for tim in period_time:
                    if fuzz.token_set_ratio(tim.strip(), 'valid for three 3 years from')>0.7 or fuzz.token_set_ratio(tim.strip(), 'có giá trị ba 3 năm từ'):
                        time_end = max(date_start) + datetime.timedelta(days=1096)
                        if time_end > datetime.datetime.now():
                            comments.append({ 
                                "commentContent": f"Thời gian còn hiệu lực (trang {date[-9:]})",
                                "commentStatus" : "OK",
                                "date_end": date_end,
                                "date_start": [t.strftime('%m/%d/%Y') for t in date_start],
                                "valid_date": valid_date,
                                "begin_date": begin_date,
                                "period": period_time
                            })
                            date_flag = 1
                            break

            else:
                comments.append({ 
                            "commentContent": f"Thời gian hết hiệu lực",
                            "commentStatus" : "NOK",
                            "date_end": date_end,
                            "date_start": [t.strftime('%m/%d/%Y') for t in date_start],
                            "valid_date": valid_date,
                            "begin_date": begin_date,
                            "period": period_time
                        })
                err.append(6)
            
            if not date_flag:
                comments.append({ 
                        "commentContent": f"Thời gian hết hiệu lực",
                        "commentStatus" : "NOK",
                        "date_end": date_end,
                        "date_start": [t.strftime('%m/%d/%Y') for t in date_start],
                        "valid_date": valid_date,
                        "begin_date": begin_date,
                        "period": period_time
                    })
                err.append(6)
            # print('time_end: ', date_start)
            
            # ####
            # if lsq_detect:
            #     comments.append({
            #         "commentContent" : f"Có dấu lãnh sứ quán (trang {page_detect + 1})",
            #         "commentStatus" : "OK"
            #     })
            # else:
            #     comments.append({
            #         "commentContent" : f"Không có dấu lãnh sứ quán)",
            #         "commentStatus" : "NOK"
            #     })
            doc_type = 2
            del_data()
            
        return comments, doc_type, err



if __name__ == '__main__':

    cfs = CFS()
    pdf_path = 'test1.pdf'
    # pdf_info = cfs.tool_reader(pdf_path)
    # result = cfs.get_ptn_info(pdf_info)
    # print(result)

    es = Elasticsearch(hosts=["http://localhost:9200"], timeout=60, retry_on_timeout=True)

    vertical_results, horizontal_results = cfs.deeplearning_reader(pdf_path)
    print('--'*20)
    print(vertical_results)
    push_result(es, vertical_results)
    query = '760-4528'
    result = search(es,query,1)
    print(result)



    # print(pdf_info)

